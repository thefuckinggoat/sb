package database

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// SortField represents a sort specification
type SortField struct {
	Field      string
	Descending bool
}

// WriteModel represents a bulk write operation
type WriteModel struct {
	Type        string // "insert", "update", "delete", "replace"
	Filter      map[string]interface{}
	Update      map[string]interface{}
	Document    map[string]interface{}
	Replacement map[string]interface{}
	Upsert      bool
}

// BulkUpdateOp represents a single update operation for bulk updates
type BulkUpdateOp struct {
	Filter map[string]interface{}
	Update map[string]interface{}
	Upsert bool
}

// BulkWriteResult holds results from a bulk write
type BulkWriteResult struct {
	InsertedCount int64
	ModifiedCount int64
	DeletedCount  int64
	UpsertedCount int64
}

// BatchProcessor batches operations for efficient bulk writes
type BatchProcessor struct {
	mu                sync.Mutex
	collection        string
	batchSize         int
	ordered           bool
	requests          []mongo.WriteModel
	stats             BatchStats
	lastExecution     time.Time
	connectionTimeout time.Duration
}

// BatchStats tracks batch operation statistics
type BatchStats struct {
	Inserts         int
	Updates         int
	Deletes         int
	Errors          int
	BatchesExecuted int
}

// Database config structs
type dbConfig struct {
	MongoDB mongoDBConfig `json:"mongodb"`
}

type mongoDBConfig struct {
	URI      string                 `json:"uri"`
	Database string                 `json:"database"`
	Options  map[string]interface{} `json:"options"`
}

// Global variables
var (
	globalClient         *mongo.Client
	globalDB             *mongo.Database
	globalConnectionActive bool
	globalHealthCancel   context.CancelFunc
	connectionString     string
	dbName               string
	globalMu             sync.RWMutex
)

// InitializeGlobalDatabase initializes the global MongoDB connection
func InitializeGlobalDatabase() error {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalConnectionActive {
		log.Println("[GlobalDB] Database already initialized")
		return nil
	}

	// Load configuration
	if err := loadGlobalConfig(); err != nil {
		return fmt.Errorf("[GlobalDB] Failed to load config: %w", err)
	}

	log.Println("[GlobalDB] Creating global MongoDB connection...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(connectionString)

	// Apply connection pool settings
	clientOpts.SetMaxPoolSize(100)
	clientOpts.SetMinPoolSize(10)
	clientOpts.SetMaxConnIdleTime(120 * time.Second)
	clientOpts.SetServerSelectionTimeout(10 * time.Second)
	clientOpts.SetRetryReads(true)
	clientOpts.SetRetryWrites(true)
	clientOpts.SetReadPreference(readpref.Primary())
	clientOpts.SetCompressors([]string{"zstd"})

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("[GlobalDB] Failed to connect: %w", err)
	}

	// Test connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		client.Disconnect(ctx)
		return fmt.Errorf("[GlobalDB] Ping failed: %w", err)
	}

	globalClient = client
	globalDB = client.Database(dbName)
	globalConnectionActive = true

	// Create indexes
	if err := createGlobalIndexes(); err != nil {
		log.Printf("[GlobalDB] Warning: index creation had errors: %v", err)
	}

	// Start health check
	healthCtx, healthCancel := context.WithCancel(context.Background())
	globalHealthCancel = healthCancel
	go globalHealthCheck(healthCtx)

	log.Println("[GlobalDB] Global MongoDB connection established successfully")
	return nil
}

func loadGlobalConfig() error {
	configPath := filepath.Join("config", "database.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read database config: %w", err)
	}

	var cfg dbConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("failed to parse database config: %w", err)
	}

	connectionString = cfg.MongoDB.URI
	dbName = cfg.MongoDB.Database

	log.Println("[GlobalDB] Loaded global MongoDB configuration")
	return nil
}

func createGlobalIndexes() error {
	log.Println("[GlobalDB] Checking and creating database indexes...")

	type indexDef struct {
		Collection string
		Keys       bson.D
		Options    *options.IndexOptions
	}

	indexes := []indexDef{
		// username_history
		{
			Collection: "username_history",
			Keys:       bson.D{{Key: "user_id", Value: 1}, {Key: "value", Value: 1}},
			Options:    options.Index().SetUnique(true).SetBackground(true),
		},
		// display_name_history
		{
			Collection: "display_name_history",
			Keys:       bson.D{{Key: "user_id", Value: 1}, {Key: "value", Value: 1}},
			Options:    options.Index().SetUnique(true).SetBackground(true),
		},
		// user_messages: user_id + created_at
		{
			Collection: "user_messages",
			Keys:       bson.D{{Key: "user_id", Value: 1}, {Key: "created_at", Value: -1}},
			Options:    options.Index().SetBackground(true),
		},
		// user_messages: guild_id + user_id + created_at
		{
			Collection: "user_messages",
			Keys:       bson.D{{Key: "guild_id", Value: 1}, {Key: "user_id", Value: 1}, {Key: "created_at", Value: -1}},
			Options:    options.Index().SetBackground(true),
		},
		// user_messages: channel_id + created_at
		{
			Collection: "user_messages",
			Keys:       bson.D{{Key: "channel_id", Value: 1}, {Key: "created_at", Value: -1}},
			Options:    options.Index().SetBackground(true),
		},
		// authorized_hosts
		{
			Collection: "authorized_hosts",
			Keys:       bson.D{{Key: "user_id", Value: 1}},
			Options:    options.Index().SetUnique(true).SetBackground(true),
		},
		// hosted_tokens
		{
			Collection: "hosted_tokens",
			Keys:       bson.D{{Key: "host_user_id", Value: 1}, {Key: "token_owner_id", Value: 1}},
			Options:    options.Index().SetUnique(true).SetBackground(true),
		},
		// blacklisted_users
		{
			Collection: "blacklisted_users",
			Keys:       bson.D{{Key: "user_id", Value: 1}},
			Options:    options.Index().SetUnique(true).SetBackground(true),
		},
	}

	// Collection-specific indexes for deleted_messages, edited_messages, mentions
	for _, collection := range []string{"deleted_messages", "edited_messages", "mentions"} {
		// message_id unique index
		indexes = append(indexes, indexDef{
			Collection: collection,
			Keys:       bson.D{{Key: "message_id", Value: 1}},
			Options:    options.Index().SetUnique(true).SetBackground(true),
		})

		// Compound index: user_id + channel_id + guild_id
		indexes = append(indexes, indexDef{
			Collection: collection,
			Keys:       bson.D{{Key: "user_id", Value: 1}, {Key: "channel_id", Value: 1}, {Key: "guild_id", Value: 1}},
			Options:    options.Index().SetBackground(true),
		})
	}

	// Time-based indexes per collection
	// deleted_messages: user_id + deleted_at
	indexes = append(indexes, indexDef{
		Collection: "deleted_messages",
		Keys:       bson.D{{Key: "user_id", Value: 1}, {Key: "deleted_at", Value: -1}},
		Options:    options.Index().SetBackground(true),
	})
	// edited_messages: user_id + edited_at
	indexes = append(indexes, indexDef{
		Collection: "edited_messages",
		Keys:       bson.D{{Key: "user_id", Value: 1}, {Key: "edited_at", Value: -1}},
		Options:    options.Index().SetBackground(true),
	})
	// mentions: target_id + mentioner_id + created_at
	indexes = append(indexes, indexDef{
		Collection: "mentions",
		Keys:       bson.D{{Key: "target_id", Value: 1}, {Key: "mentioner_id", Value: 1}, {Key: "created_at", Value: -1}},
		Options:    options.Index().SetBackground(true),
	})

	// TTL indexes
	ttl48h := int32(48 * 60 * 60)
	indexes = append(indexes, indexDef{
		Collection: "deleted_messages",
		Keys:       bson.D{{Key: "deleted_at", Value: -1}, {Key: "channel_id", Value: 1}},
		Options:    options.Index().SetExpireAfterSeconds(ttl48h).SetBackground(true),
	})
	indexes = append(indexes, indexDef{
		Collection: "edited_messages",
		Keys:       bson.D{{Key: "edited_at", Value: -1}, {Key: "channel_id", Value: 1}},
		Options:    options.Index().SetExpireAfterSeconds(ttl48h).SetBackground(true),
	})
	indexes = append(indexes, indexDef{
		Collection: "mentions",
		Keys:       bson.D{{Key: "expires_at", Value: 1}},
		Options:    options.Index().SetExpireAfterSeconds(ttl48h).SetBackground(true),
	})

	// Create all indexes
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	successCount := 0
	errorCount := 0

	for _, idx := range indexes {
		model := mongo.IndexModel{
			Keys:    idx.Keys,
			Options: idx.Options,
		}
		_, err := globalDB.Collection(idx.Collection).Indexes().CreateOne(ctx, model)
		if err != nil {
			// Ignore duplicate index errors
			if !mongo.IsDuplicateKeyError(err) {
				log.Printf("[GlobalDB] Index creation warning for %s: %v", idx.Collection, err)
				errorCount++
			}
		} else {
			successCount++
		}
	}

	log.Printf("[GlobalDB] Index creation completed: %d successful, %d errors", successCount, errorCount)

	if errorCount == 0 {
		log.Println("[GlobalDB] ✅ All indexes created successfully - optimal performance enabled!")
	}

	return nil
}

func globalHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(120 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
			err := globalClient.Ping(pingCtx, readpref.Primary())
			pingCancel()

			if err != nil {
				log.Printf("[GlobalDB] Health check failed: %v", err)
				globalMu.Lock()
				globalConnectionActive = false
				globalMu.Unlock()

				// Attempt reinit if connection lost
				if isConnectionLost(err) {
					log.Println("[GlobalDB] Connection lost - attempting to reinitialize")
					cleanupGlobalDatabase()
					if reinitErr := InitializeGlobalDatabase(); reinitErr != nil {
						log.Printf("[GlobalDB] Failed to reinitialize: %v", reinitErr)
						time.Sleep(10 * time.Second)
					}
				}
			} else {
				globalMu.Lock()
				if !globalConnectionActive {
					log.Println("[GlobalDB] Global MongoDB connection restored")
					globalConnectionActive = true
				}
				globalMu.Unlock()
			}
		}
	}
}

func isConnectionLost(err error) bool {
	errStr := err.Error()
	return contains(errStr, "connection closed") ||
		contains(errStr, "connection reset") ||
		contains(errStr, "EOF")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func cleanupGlobalDatabase() {
	if globalHealthCancel != nil {
		globalHealthCancel()
		globalHealthCancel = nil
	}

	if globalClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		globalClient.Disconnect(ctx)
		globalClient = nil
		globalDB = nil
	}

	globalConnectionActive = false
	log.Println("[GlobalDB] Cleaned up global MongoDB resources")
}

// ShutdownGlobalDatabase shuts down the global database connection
func ShutdownGlobalDatabase() {
	log.Println("[GlobalDB] Shutting down global database...")
	globalMu.Lock()
	defer globalMu.Unlock()
	cleanupGlobalDatabase()
}

// GetGlobalDB returns the global database instance
func GetGlobalDB() *mongo.Database {
	globalMu.RLock()
	defer globalMu.RUnlock()
	if !globalConnectionActive || globalDB == nil {
		return nil
	}
	return globalDB
}

// IsGlobalDBActive returns whether the global database is active
func IsGlobalDBActive() bool {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalConnectionActive
}

func getWriteConcernCollection(collection string) *mongo.Collection {
	wc := writeconcern.Majority()
	opts := options.Collection().SetWriteConcern(wc)
	return globalDB.Collection(collection, opts)
}

// ─── Public API ───

// GlobalFindOne performs a find_one operation
func GlobalFindOne(collection string, query map[string]interface{}, projection map[string]interface{}) (map[string]interface{}, error) {
	if !IsGlobalDBActive() {
		return nil, fmt.Errorf("[GlobalDB] Global database not active")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.FindOne()
	if projection != nil {
		opts.SetProjection(projection)
	}

	var result map[string]interface{}
	err := globalDB.Collection(collection).FindOne(ctx, query, opts).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return result, nil
}

// GlobalUpdateOne updates one document
func GlobalUpdateOne(collection string, filter map[string]interface{}, update map[string]interface{}, upsert bool) error {
	if !IsGlobalDBActive() {
		return fmt.Errorf("[GlobalDB] Global database not active")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.Update().SetUpsert(upsert)
	_, err := getWriteConcernCollection(collection).UpdateOne(ctx, filter, update, opts)
	return err
}

// GlobalUpdateMany updates many documents
func GlobalUpdateMany(collection string, filter map[string]interface{}, update map[string]interface{}, upsert bool) error {
	if !IsGlobalDBActive() {
		return fmt.Errorf("[GlobalDB] Global database not active")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	opts := options.Update().SetUpsert(upsert)
	_, err := getWriteConcernCollection(collection).UpdateMany(ctx, filter, update, opts)
	return err
}

// GlobalInsertOne inserts one document
func GlobalInsertOne(collection string, document map[string]interface{}) error {
	if !IsGlobalDBActive() {
		return fmt.Errorf("[GlobalDB] Global database not active")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := getWriteConcernCollection(collection).InsertOne(ctx, document)
	return err
}

// GlobalDeleteOne deletes one document
func GlobalDeleteOne(collection string, filter map[string]interface{}) error {
	if !IsGlobalDBActive() {
		return fmt.Errorf("[GlobalDB] Global database not active")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := getWriteConcernCollection(collection).DeleteOne(ctx, filter)
	return err
}

// GlobalFindMany finds many documents
func GlobalFindMany(collection string, query map[string]interface{}, projection map[string]interface{},
	sort []SortField, limit int, skip int) ([]map[string]interface{}, error) {

	if !IsGlobalDBActive() {
		return nil, fmt.Errorf("[GlobalDB] Global database not active")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if query == nil {
		query = map[string]interface{}{}
	}

	opts := options.Find()
	if projection != nil {
		opts.SetProjection(projection)
	}
	if sort != nil {
		sortDoc := bson.D{}
		for _, s := range sort {
			order := 1
			if s.Descending {
				order = -1
			}
			sortDoc = append(sortDoc, bson.E{Key: s.Field, Value: order})
		}
		opts.SetSort(sortDoc)
	}
	if skip > 0 {
		opts.SetSkip(int64(skip))
	}
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}

	cursor, err := globalDB.Collection(collection).Find(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	if results == nil {
		results = []map[string]interface{}{}
	}
	return results, nil
}

// GlobalAggregate performs an aggregation
func GlobalAggregate(collection string, pipeline []map[string]interface{}) ([]map[string]interface{}, error) {
	if !IsGlobalDBActive() {
		return nil, fmt.Errorf("[GlobalDB] Global database not active")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	opts := options.Aggregate().SetAllowDiskUse(true)

	// Convert pipeline to bson
	bsonPipeline := make([]bson.M, len(pipeline))
	for i, stage := range pipeline {
		bsonPipeline[i] = bson.M(stage)
	}

	cursor, err := globalDB.Collection(collection).Aggregate(ctx, bsonPipeline, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	if results == nil {
		results = []map[string]interface{}{}
	}
	return results, nil
}

// GlobalCountDocuments counts documents
func GlobalCountDocuments(collection string, filter map[string]interface{}) (int64, error) {
	if !IsGlobalDBActive() {
		return 0, fmt.Errorf("[GlobalDB] Global database not active")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if filter == nil {
		filter = map[string]interface{}{}
	}

	return globalDB.Collection(collection).CountDocuments(ctx, filter)
}

// GlobalBulkWrite performs bulk write operations
func GlobalBulkWrite(collection string, requests []WriteModel, ordered bool) (*BulkWriteResult, error) {
	if !IsGlobalDBActive() {
		return nil, fmt.Errorf("[GlobalDB] Global database not active")
	}

	if len(requests) == 0 {
		return nil, nil
	}

	// Split large batches
	maxBatchSize := 100
	if len(requests) > maxBatchSize {
		log.Printf("[GlobalDB] Large batch (%d operations) being split for connection stability", len(requests))

		totalResult := &BulkWriteResult{}
		for i := 0; i < len(requests); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(requests) {
				end = len(requests)
			}
			batch := requests[i:end]
			result, err := GlobalBulkWrite(collection, batch, ordered)
			if err != nil {
				return totalResult, err
			}
			if result != nil {
				totalResult.InsertedCount += result.InsertedCount
				totalResult.ModifiedCount += result.ModifiedCount
				totalResult.DeletedCount += result.DeletedCount
				totalResult.UpsertedCount += result.UpsertedCount
			}
		}
		return totalResult, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Convert WriteModel to mongo.WriteModel
	var models []mongo.WriteModel
	for _, req := range requests {
		switch req.Type {
		case "insert":
			models = append(models, mongo.NewInsertOneModel().SetDocument(req.Document))
		case "update":
			m := mongo.NewUpdateOneModel().SetFilter(req.Filter).SetUpdate(req.Update).SetUpsert(req.Upsert)
			models = append(models, m)
		case "delete":
			models = append(models, mongo.NewDeleteOneModel().SetFilter(req.Filter))
		case "replace":
			m := mongo.NewReplaceOneModel().SetFilter(req.Filter).SetReplacement(req.Replacement).SetUpsert(req.Upsert)
			models = append(models, m)
		}
	}

	opts := options.BulkWrite().SetOrdered(ordered)
	result, err := getWriteConcernCollection(collection).BulkWrite(ctx, models, opts)
	if err != nil {
		log.Printf("[GlobalDB] Bulk write error in %s: %v", collection, err)
		return nil, err
	}

	bulkResult := &BulkWriteResult{
		InsertedCount: int64(result.InsertedCount),
		ModifiedCount: int64(result.ModifiedCount),
		DeletedCount:  int64(result.DeletedCount),
		UpsertedCount: int64(result.UpsertedCount),
	}

	if len(requests) > 50 {
		log.Printf("[GlobalDB] Completed bulk write: %d operations to %s", len(requests), collection)
	}

	return bulkResult, nil
}

// GlobalInsertMany inserts many documents
func GlobalInsertMany(collection string, documents []map[string]interface{}, ordered bool) (*BulkWriteResult, error) {
	if len(documents) == 0 {
		return nil, nil
	}

	var requests []WriteModel
	for _, doc := range documents {
		requests = append(requests, WriteModel{Type: "insert", Document: doc})
	}
	return GlobalBulkWrite(collection, requests, ordered)
}

// GlobalUpdateManyBulk performs multiple update operations in a single bulk write
func GlobalUpdateManyBulk(collection string, operations []BulkUpdateOp, ordered bool) (*BulkWriteResult, error) {
	if len(operations) == 0 {
		return nil, nil
	}

	var requests []WriteModel
	for _, op := range operations {
		requests = append(requests, WriteModel{
			Type:   "update",
			Filter: op.Filter,
			Update: op.Update,
			Upsert: op.Upsert,
		})
	}
	return GlobalBulkWrite(collection, requests, ordered)
}

// ─── BatchProcessor ───

func NewBatchProcessor(collection string, batchSize int, ordered bool) *BatchProcessor {
	if batchSize == 0 {
		batchSize = 100
	}
	return &BatchProcessor{
		collection:        collection,
		batchSize:         batchSize,
		ordered:           ordered,
		requests:          make([]mongo.WriteModel, 0),
		connectionTimeout: 30 * time.Second,
	}
}

func (bp *BatchProcessor) AddInsert(document map[string]interface{}) int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.requests = append(bp.requests, mongo.NewInsertOneModel().SetDocument(document))
	bp.stats.Inserts++
	return len(bp.requests)
}

func (bp *BatchProcessor) AddUpdate(filter map[string]interface{}, update map[string]interface{}, upsert bool) int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.requests = append(bp.requests, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(upsert))
	bp.stats.Updates++
	return len(bp.requests)
}

func (bp *BatchProcessor) AddDelete(filter map[string]interface{}) int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.requests = append(bp.requests, mongo.NewDeleteOneModel().SetFilter(filter))
	bp.stats.Deletes++
	return len(bp.requests)
}

func (bp *BatchProcessor) AddReplace(filter map[string]interface{}, replacement map[string]interface{}, upsert bool) int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.requests = append(bp.requests, mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(replacement).SetUpsert(upsert))
	bp.stats.Updates++
	return len(bp.requests)
}

func (bp *BatchProcessor) ExecuteBatch(force bool) (*BulkWriteResult, int, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if len(bp.requests) == 0 || (!force && len(bp.requests) < bp.batchSize) {
		return nil, 0, nil
	}

	// Force if connection has been idle too long
	if !bp.lastExecution.IsZero() && time.Since(bp.lastExecution) > bp.connectionTimeout {
		force = true
	}

	if !IsGlobalDBActive() {
		log.Printf("[BatchProcessor] Global DB inactive, skipping batch of %d operations", len(bp.requests))
		return nil, 0, nil
	}

	requestsCopy := make([]mongo.WriteModel, len(bp.requests))
	copy(requestsCopy, bp.requests)
	executedCount := len(requestsCopy)
	bp.requests = bp.requests[:0]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := options.BulkWrite().SetOrdered(bp.ordered)
	result, err := getWriteConcernCollection(bp.collection).BulkWrite(ctx, requestsCopy, opts)

	bp.lastExecution = time.Now()
	bp.stats.BatchesExecuted++

	if err != nil {
		bp.stats.Errors++
		return nil, executedCount, err
	}

	bulkResult := &BulkWriteResult{
		InsertedCount: int64(result.InsertedCount),
		ModifiedCount: int64(result.ModifiedCount),
		DeletedCount:  int64(result.DeletedCount),
		UpsertedCount: int64(result.UpsertedCount),
	}

	if executedCount > 10 {
		log.Printf("[BatchProcessor] Executed batch: %d operations to %s", executedCount, bp.collection)
	}

	return bulkResult, executedCount, nil
}

func (bp *BatchProcessor) Flush() (*BulkWriteResult, int, error) {
	result, count, err := bp.ExecuteBatch(true)
	bp.mu.Lock()
	bp.requests = bp.requests[:0]
	bp.mu.Unlock()
	return result, count, err
}

func (bp *BatchProcessor) Size() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.requests)
}

func (bp *BatchProcessor) IsReady() bool {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.requests) >= bp.batchSize
}

func (bp *BatchProcessor) GetStats() map[string]interface{} {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return map[string]interface{}{
		"inserts":            bp.stats.Inserts,
		"updates":            bp.stats.Updates,
		"deletes":            bp.stats.Deletes,
		"errors":             bp.stats.Errors,
		"batches_executed":   bp.stats.BatchesExecuted,
		"pending_operations": len(bp.requests),
		"collection":         bp.collection,
	}
}

func (bp *BatchProcessor) ResetStats() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.stats = BatchStats{}
}