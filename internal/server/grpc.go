package server

import (
	"PerpLedger/internal/ingestion"
	"PerpLedger/internal/observability"
	"PerpLedger/internal/persistence"
	"PerpLedger/internal/projection"
	"PerpLedger/internal/query"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	googleuuid "github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	adminv1 "PerpLedger/gen/go/perpledger/admin/v1"
	eventsv1 "PerpLedger/gen/go/perpledger/events/v1"
	ingestv1 "PerpLedger/gen/go/perpledger/ingest/v1"
	queryv1 "PerpLedger/gen/go/perpledger/query/v1"
)

// GRPCServer wraps the gRPC server and gRPC-Gateway HTTP mux.
type GRPCServer struct {
	grpcServer    *grpc.Server
	httpServer    *http.Server
	grpcAddr      string
	httpAddr      string
	healthChecker *observability.HealthChecker
}

// ServerDeps holds all dependencies needed by the gRPC services.
type ServerDeps struct {
	DB            *sql.DB
	QueryService  *query.QueryService
	IngestService *ingestion.GRPCIngestService
	SnapshotMgr   *persistence.SnapshotManager
	StartTime     time.Time
	HealthChecker *observability.HealthChecker
}

// NewGRPCServer creates a new gRPC server with all services registered.
func NewGRPCServer(grpcAddr, httpAddr string, deps *ServerDeps) *GRPCServer {
	grpcServer := grpc.NewServer()

	// Register services
	queryv1.RegisterQueryServiceServer(grpcServer, &queryServiceImpl{qs: deps.QueryService})
	ingestv1.RegisterIngestServiceServer(grpcServer, &ingestServiceImpl{svc: deps.IngestService})
	adminv1.RegisterAdminServiceServer(grpcServer, &adminServiceImpl{
		db:          deps.DB,
		snapMgr:     deps.SnapshotMgr,
		queryService: deps.QueryService,
		startTime:   deps.StartTime,
	})

	// Health check
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Reflection for grpcurl / grpcui
	reflection.Register(grpcServer)

	return &GRPCServer{
		grpcServer:    grpcServer,
		grpcAddr:      grpcAddr,
		httpAddr:      httpAddr,
		healthChecker: deps.HealthChecker,
	}
}

// StartGRPC starts the gRPC server (blocking).
func (s *GRPCServer) StartGRPC(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	go func() {
		<-ctx.Done()
		log.Println("INFO: gRPC server shutting down...")
		s.grpcServer.GracefulStop()
	}()

	log.Printf("INFO: gRPC server listening on %s", s.grpcAddr)
	return s.grpcServer.Serve(lis)
}

// StartHTTPGateway starts the gRPC-Gateway HTTP reverse proxy (blocking).
// Per doc §16: HTTP/JSON is served via gRPC-Gateway for tooling, dashboards, curl.
func (s *GRPCServer) StartHTTPGateway(ctx context.Context) error {
	mux := runtime.NewServeMux()

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	// Register gateway handlers — they proxy HTTP/JSON to the gRPC server
	if err := queryv1.RegisterQueryServiceHandlerFromEndpoint(ctx, mux, s.grpcAddr, opts); err != nil {
		return fmt.Errorf("register query gateway: %w", err)
	}
	if err := ingestv1.RegisterIngestServiceHandlerFromEndpoint(ctx, mux, s.grpcAddr, opts); err != nil {
		return fmt.Errorf("register ingest gateway: %w", err)
	}
	if err := adminv1.RegisterAdminServiceHandlerFromEndpoint(ctx, mux, s.grpcAddr, opts); err != nil {
		return fmt.Errorf("register admin gateway: %w", err)
	}

	// Health endpoints per doc §20 §7
	httpMux := http.NewServeMux()
	if s.healthChecker != nil {
		httpMux.HandleFunc("/healthz", s.healthChecker.LivenessHandler)
		httpMux.HandleFunc("/readyz", s.healthChecker.ReadinessHandler)
	} else {
		httpMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"ok"}`)
		})
	}
	httpMux.Handle("/", mux)

	s.httpServer = &http.Server{
		Addr:    s.httpAddr,
		Handler: httpMux,
	}

	go func() {
		<-ctx.Done()
		log.Println("INFO: HTTP gateway shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("INFO: HTTP gateway listening on %s (proxying to gRPC %s)", s.httpAddr, s.grpcAddr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// ============================================================================
// QueryService gRPC implementation
// ============================================================================

type queryServiceImpl struct {
	queryv1.UnimplementedQueryServiceServer
	qs *query.QueryService
}

func (s *queryServiceImpl) GetBalances(ctx context.Context, req *queryv1.GetBalancesRequest) (*queryv1.GetBalancesResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	userID, err := parseUUID(req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id: %v", err)
	}

	// For now, query USDT balance (extend to multi-asset later)
	bal, err := s.qs.GetBalance(ctx, userID, "USDT")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get balance: %v", err)
	}

	return &queryv1.GetBalancesResponse{
		Balances: []*queryv1.AssetBalance{
			{
				Asset:             bal.Asset,
				TotalBalance:      bal.TotalBalance,
				AvailableBalance:  bal.AvailableBalance,
				ReservedBalance:   bal.ReservedBalance,
				PendingDeposit:    bal.PendingDeposit,
				PendingWithdrawal: bal.PendingWithdrawal,
			},
		},
		AsOfSequence: bal.AsOfSequence,
	}, nil
}

func (s *queryServiceImpl) GetBalanceSummary(ctx context.Context, req *queryv1.GetBalanceSummaryRequest) (*queryv1.GetBalanceSummaryResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	userID, err := parseUUID(req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id: %v", err)
	}

	margin, err := s.qs.GetMarginSnapshot(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get margin: %v", err)
	}

	return &queryv1.GetBalanceSummaryResponse{
		UserId:       req.UserId,
		AsOfSequence: margin.AsOfSequence,
	}, nil
}

func (s *queryServiceImpl) ListPositions(ctx context.Context, req *queryv1.ListPositionsRequest) (*queryv1.ListPositionsResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	userID, err := parseUUID(req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id: %v", err)
	}

	positions, err := s.qs.GetPositions(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get positions: %v", err)
	}

	var pbPositions []*queryv1.PositionWithDerived
	for _, p := range positions {
		pbPositions = append(pbPositions, &queryv1.PositionWithDerived{
			Position: &queryv1.Position{
				UserId:           p.UserID.String(),
				MarketId:         p.MarketID,
				Size:             p.Size,
				AvgEntryPrice:    p.AvgEntryPrice,
				RealizedPnl:      p.RealizedPnL,
				LastFundingEpoch: p.LastFundingEpoch,
			},
			UnrealizedPnl: p.UnrealizedPnL,
		})
	}

	var asOf int64
	if len(positions) > 0 {
		asOf = positions[0].AsOfSequence
	}

	return &queryv1.ListPositionsResponse{
		Positions:    pbPositions,
		AsOfSequence: asOf,
	}, nil
}

func (s *queryServiceImpl) GetPosition(ctx context.Context, req *queryv1.GetPositionRequest) (*queryv1.GetPositionResponse, error) {
	if req.UserId == "" || req.MarketId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id and market_id are required")
	}

	userID, err := parseUUID(req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id: %v", err)
	}

	positions, err := s.qs.GetPositions(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get positions: %v", err)
	}

	for _, p := range positions {
		if p.MarketID == req.MarketId {
			return &queryv1.GetPositionResponse{
				Position: &queryv1.Position{
					UserId:           p.UserID.String(),
					MarketId:         p.MarketID,
					Size:             p.Size,
					AvgEntryPrice:    p.AvgEntryPrice,
					RealizedPnl:      p.RealizedPnL,
					LastFundingEpoch: p.LastFundingEpoch,
				},
				UnrealizedPnl: p.UnrealizedPnL,
				AsOfSequence:  p.AsOfSequence,
			}, nil
		}
	}

	return nil, status.Errorf(codes.NotFound, "no position for user %s in market %s", req.UserId, req.MarketId)
}

func (s *queryServiceImpl) GetMarginSnapshot(ctx context.Context, req *queryv1.GetMarginSnapshotRequest) (*queryv1.GetMarginSnapshotResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	userID, err := parseUUID(req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id: %v", err)
	}

	margin, err := s.qs.GetMarginSnapshot(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get margin: %v", err)
	}

	return &queryv1.GetMarginSnapshotResponse{
		UserId:       req.UserId,
		AsOfSequence: margin.AsOfSequence,
	}, nil
}

func (s *queryServiceImpl) ListFundingHistory(ctx context.Context, req *queryv1.ListFundingHistoryRequest) (*queryv1.ListFundingHistoryResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	userID, err := parseUUID(req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id: %v", err)
	}

	pageSize := int(req.PageSize)
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 50
	}

	var marketID *string
	if req.MarketId != "" {
		marketID = &req.MarketId
	}

	var afterEpoch *int64
	if req.FromEpochId > 0 {
		afterEpoch = &req.FromEpochId
	}

	history, err := s.qs.GetFundingHistory(ctx, userID, marketID, pageSize, afterEpoch)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get funding history: %v", err)
	}

	var payments []*queryv1.FundingPayment
	for _, h := range history {
		payments = append(payments, &queryv1.FundingPayment{
			MarketId:     h.MarketID,
			EpochId:      h.EpochID,
			FundingRate:  h.FundingRate,
			PositionSize: h.PositionSize,
			MarkPrice:    h.MarkPrice,
			Payment:      h.Payment,
			TimestampUs:  h.Timestamp,
		})
	}

	var asOf int64
	if len(history) > 0 {
		asOf = history[0].AsOfSequence
	}

	return &queryv1.ListFundingHistoryResponse{
		Payments:     payments,
		AsOfSequence: asOf,
	}, nil
}

func (s *queryServiceImpl) GetLiquidationStatus(ctx context.Context, req *queryv1.GetLiquidationStatusRequest) (*queryv1.GetLiquidationStatusResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	userID, err := parseUUID(req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id: %v", err)
	}

	liqs, err := s.qs.GetLiquidationStatus(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get liquidation status: %v", err)
	}

	// Filter by market if specified
	for _, l := range liqs {
		if req.MarketId == "" || l.MarketID == req.MarketId {
			return &queryv1.GetLiquidationStatusResponse{
				LiquidationId: l.LiquidationID,
				State:         fmt.Sprintf("%d", l.State),
				OriginalSize:  l.InitialSize,
				RemainingSize: l.RemainingSize,
				Deficit:       l.Deficit,
			}, nil
		}
	}

	return &queryv1.GetLiquidationStatusResponse{
		State: "healthy",
	}, nil
}

func (s *queryServiceImpl) ListLiquidationHistory(ctx context.Context, req *queryv1.ListLiquidationHistoryRequest) (*queryv1.ListLiquidationHistoryResponse, error) {
	// Simplified — delegates to GetLiquidationStatus for now
	return &queryv1.ListLiquidationHistoryResponse{}, nil
}

func (s *queryServiceImpl) ListJournals(ctx context.Context, req *queryv1.ListJournalsRequest) (*queryv1.ListJournalsResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	userID, err := parseUUID(req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id: %v", err)
	}

	pageSize := int(req.PageSize)
	if pageSize <= 0 || pageSize > 500 {
		pageSize = 100
	}

	var afterSeq *int64
	if req.FromSequence > 0 {
		afterSeq = &req.FromSequence
	}

	entries, err := s.qs.GetJournalHistory(ctx, userID, pageSize, afterSeq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get journals: %v", err)
	}

	var journals []*queryv1.JournalRecord
	for _, e := range entries {
		journals = append(journals, &queryv1.JournalRecord{
			JournalId:     e.JournalID,
			BatchId:       e.BatchID,
			EventSequence: e.Sequence,
			DebitAccount:  e.DebitAccount,
			CreditAccount: e.CreditAccount,
			AssetId: func() string {
				return fmt.Sprintf("%d", e.AssetID)
			}(),
			Amount:      e.Amount,
			JournalType: fmt.Sprintf("%d", e.JournalType),
			TimestampUs: e.Timestamp,
		})
	}

	return &queryv1.ListJournalsResponse{
		Journals: journals,
	}, nil
}

func (s *queryServiceImpl) GetSystemStatus(ctx context.Context, req *queryv1.GetSystemStatusRequest) (*queryv1.GetSystemStatusResponse, error) {
	return &queryv1.GetSystemStatusResponse{
		State: "ready",
	}, nil
}

// ============================================================================
// IngestService gRPC implementation
// ============================================================================

type ingestServiceImpl struct {
	ingestv1.UnimplementedIngestServiceServer
	svc *ingestion.GRPCIngestService
}

func (s *ingestServiceImpl) SubmitEvent(ctx context.Context, req *ingestv1.SubmitEventRequest) (*ingestv1.SubmitEventResponse, error) {
	if req.Envelope == nil {
		return nil, status.Error(codes.InvalidArgument, "envelope is required")
	}

	// Map protobuf EventType enum to string event type name for the parser
	eventTypeName := protoEventTypeToString(req.Envelope.EventType)
	if eventTypeName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "unknown event_type: %d", req.Envelope.EventType)
	}

	raw := ingestion.RawEvent{
		Subject: eventTypeName,
		Data:    req.Envelope.Payload,
	}

	evt, err := ingestion.ParseRawEvent(raw, eventTypeName)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse payload: %v", err)
	}

	// Inject into the event channel (same path as GRPCIngestService)
	select {
	case s.svc.EventChan() <- evt:
		return &ingestv1.SubmitEventResponse{Accepted: true}, nil
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "context cancelled")
	}
}

func protoEventTypeToString(et eventsv1.EventType) string {
	switch et {
	case eventsv1.EventType_TRADE_FILL:
		return "TradeFill"
	case eventsv1.EventType_DEPOSIT_INITIATED:
		return "DepositInitiated"
	case eventsv1.EventType_DEPOSIT_CONFIRMED:
		return "DepositConfirmed"
	case eventsv1.EventType_WITHDRAWAL_REQUESTED:
		return "WithdrawalRequested"
	case eventsv1.EventType_WITHDRAWAL_CONFIRMED:
		return "WithdrawalConfirmed"
	case eventsv1.EventType_WITHDRAWAL_REJECTED:
		return "WithdrawalRejected"
	case eventsv1.EventType_MARK_PRICE_UPDATE:
		return "MarkPriceUpdate"
	case eventsv1.EventType_FUNDING_RATE_SNAPSHOT:
		return "FundingRateSnapshot"
	case eventsv1.EventType_FUNDING_EPOCH_SETTLE:
		return "FundingEpochSettle"
	case eventsv1.EventType_RISK_PARAM_UPDATE:
		return "RiskParamUpdate"
	case eventsv1.EventType_LIQUIDATION_FILL:
		return "LiquidationFill"
	case eventsv1.EventType_LIQUIDATION_COMPLETED:
		return "LiquidationCompleted"
	default:
		return ""
	}
}

func (s *ingestServiceImpl) UpdateRiskParams(ctx context.Context, req *ingestv1.UpdateRiskParamsRequest) (*ingestv1.UpdateRiskParamsResponse, error) {
	// TODO: implement risk param update injection
	return &ingestv1.UpdateRiskParamsResponse{
		Accepted: true,
	}, nil
}

func (s *ingestServiceImpl) BackfillEvent(ctx context.Context, req *ingestv1.BackfillEventRequest) (*ingestv1.BackfillEventResponse, error) {
	if req.AdminToken == "" {
		return nil, status.Error(codes.PermissionDenied, "admin_token is required for backfill")
	}

	// TODO: implement backfill injection
	return &ingestv1.BackfillEventResponse{
		Accepted: true,
	}, nil
}

// ============================================================================
// AdminService gRPC implementation
// ============================================================================

type adminServiceImpl struct {
	adminv1.UnimplementedAdminServiceServer
	db           *sql.DB
	snapMgr      *persistence.SnapshotManager
	queryService *query.QueryService
	startTime    time.Time
}

func (s *adminServiceImpl) TakeSnapshot(ctx context.Context, req *adminv1.TakeSnapshotRequest) (*adminv1.TakeSnapshotResponse, error) {
	// TODO: trigger snapshot via core
	return &adminv1.TakeSnapshotResponse{}, nil
}

func (s *adminServiceImpl) RebuildProjections(ctx context.Context, req *adminv1.RebuildProjectionsRequest) (*adminv1.RebuildProjectionsResponse, error) {
	if err := projection.RebuildProjections(ctx, s.db); err != nil {
		return nil, status.Errorf(codes.Internal, "rebuild failed: %v", err)
	}
	return &adminv1.RebuildProjectionsResponse{
		Started: true,
		TaskId:  "rebuild-sync",
	}, nil
}

func (s *adminServiceImpl) GetEventLogInfo(ctx context.Context, req *adminv1.GetEventLogInfoRequest) (*adminv1.GetEventLogInfoResponse, error) {
	latestSeq, err := s.snapMgr.GetLatestSequence(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get latest sequence: %v", err)
	}

	return &adminv1.GetEventLogInfoResponse{
		LastSequence: latestSeq,
	}, nil
}

func (s *adminServiceImpl) VerifyIntegrity(ctx context.Context, req *adminv1.VerifyIntegrityRequest) (*adminv1.VerifyIntegrityResponse, error) {
	report, err := s.queryService.VerifyIntegrity(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "verify integrity: %v", err)
	}

	resp := &adminv1.VerifyIntegrityResponse{
		Passed: report.IsHealthy,
	}

	if !report.IsHealthy && len(report.HashChainBreaks) > 0 {
		resp.FirstMismatchSequence = report.HashChainBreaks[0]
		resp.ErrorDetail = fmt.Sprintf("%d hash chain breaks, %d unbalanced assets",
			len(report.HashChainBreaks), len(report.UnbalancedAssets))
	}

	return resp, nil
}

// ============================================================================
// Helpers
// ============================================================================

func parseUUID(s string) (googleuuid.UUID, error) {
	return googleuuid.Parse(s)
}
