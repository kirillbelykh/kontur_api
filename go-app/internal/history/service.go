package history

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

type Service struct {
	cfg            config.Config
	mu             sync.RWMutex
	syncMu         sync.Mutex
	lastSyncPullAt time.Time
	originURL      string
	syncRelPath    string
}

type filePayload struct {
	Orders     []dto.OrderRecord `json:"orders"`
	LastUpdate string            `json:"last_update"`
	CreatedBy  string            `json:"created_by"`
	UpdatedBy  string            `json:"updated_by"`
	Storage    string            `json:"storage_path"`
}

func NewService(cfg config.Config) (*Service, error) {
	service := &Service{cfg: cfg}
	if err := service.ensureFile(); err != nil {
		return nil, err
	}
	service.initSync()
	return service, nil
}

func (s *Service) ensureFile() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.cfg.HistoryPath), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(s.cfg.HistoryPath); err == nil {
		return nil
	}
	return s.save(filePayload{
		Orders:     []dto.OrderRecord{},
		LastUpdate: nowISO(),
		CreatedBy:  currentUser(),
		UpdatedBy:  currentUser(),
		Storage:    s.cfg.HistoryPath,
	})
}

func (s *Service) List(filter dto.HistoryFilter) ([]dto.OrderRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	payload, err := s.load()
	if err != nil {
		return nil, err
	}
	result := make([]dto.OrderRecord, 0, len(payload.Orders))
	search := strings.ToLower(strings.TrimSpace(filter.Search))
	status := strings.TrimSpace(filter.Status)

	for _, order := range payload.Orders {
		if filter.OnlyWithoutTSD && order.TSDCreated {
			continue
		}
		if status != "" && order.Status != status {
			continue
		}
		if search != "" {
			haystack := strings.ToLower(order.OrderName + " " + order.FullName + " " + order.GTIN + " " + order.DocumentID)
			if !strings.Contains(haystack, search) {
				continue
			}
		}
		result = append(result, order)
	}

	sortOrders(result)
	return result, nil
}

func (s *Service) CountWithoutTSD() (int, error) {
	orders, err := s.List(dto.HistoryFilter{OnlyWithoutTSD: true})
	if err != nil {
		return 0, err
	}
	return len(orders), nil
}

func (s *Service) GetByDocumentID(documentID string) (*dto.OrderRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	payload, err := s.load()
	if err != nil {
		return nil, err
	}
	for _, order := range payload.Orders {
		if order.DocumentID == documentID {
			copyOrder := order
			return &copyOrder, nil
		}
	}
	return nil, nil
}

func (s *Service) Upsert(order dto.OrderRecord) (dto.OrderRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload, err := s.load()
	if err != nil {
		return dto.OrderRecord{}, err
	}

	prepared := s.prepareOrder(order)
	updated := false
	for idx, current := range payload.Orders {
		if current.DocumentID == prepared.DocumentID && prepared.DocumentID != "" {
			payload.Orders[idx] = mergeOrders(current, prepared)
			prepared = payload.Orders[idx]
			updated = true
			break
		}
	}
	if !updated {
		payload.Orders = append(payload.Orders, prepared)
	}

	sortOrders(payload.Orders)
	if err := s.save(payload); err != nil {
		return dto.OrderRecord{}, err
	}
	go s.syncWithGitHub(false, true, "upsert")
	return prepared, nil
}

func (s *Service) MarkTsdCreated(documentID, introNumber string) (dto.OrderRecord, error) {
	current, err := s.GetByDocumentID(documentID)
	if err != nil {
		return dto.OrderRecord{}, err
	}
	if current == nil {
		return dto.OrderRecord{}, fmt.Errorf("order %s not found", documentID)
	}

	current.TSDCreated = true
	current.TSDCreatedAt = nowISO()
	current.TSDIntroNumber = introNumber
	current.TSDCreatedBy = currentUser()
	current.UpdatedAt = nowISO()
	current.UpdatedBy = currentUser()
	return s.Upsert(*current)
}

func MergePayloads(base, incoming []dto.OrderRecord) []dto.OrderRecord {
	index := make(map[string]dto.OrderRecord, len(base))
	for _, order := range base {
		index[order.DocumentID] = order
	}
	for _, order := range incoming {
		current, exists := index[order.DocumentID]
		if !exists {
			index[order.DocumentID] = order
			continue
		}
		index[order.DocumentID] = mergeOrders(current, order)
	}
	merged := make([]dto.OrderRecord, 0, len(index))
	for _, order := range index {
		merged = append(merged, order)
	}
	sortOrders(merged)
	return merged
}

func (s *Service) prepareOrder(order dto.OrderRecord) dto.OrderRecord {
	now := nowISO()
	if order.CreatedAt == "" {
		order.CreatedAt = now
	}
	if order.CreatedBy == "" {
		order.CreatedBy = currentUser()
	}
	order.UpdatedAt = now
	order.UpdatedBy = currentUser()
	return order
}

func (s *Service) load() (filePayload, error) {
	raw, err := os.ReadFile(s.cfg.HistoryPath)
	if err != nil {
		if os.IsNotExist(err) {
			return filePayload{Orders: []dto.OrderRecord{}}, nil
		}
		return filePayload{}, err
	}
	var payload filePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return filePayload{Orders: []dto.OrderRecord{}}, nil
	}
	if payload.Orders == nil {
		payload.Orders = []dto.OrderRecord{}
	}
	return payload, nil
}

func (s *Service) save(payload filePayload) error {
	payload.LastUpdate = nowISO()
	payload.UpdatedBy = currentUser()
	payload.Storage = s.cfg.HistoryPath
	if payload.CreatedBy == "" {
		payload.CreatedBy = currentUser()
	}
	if payload.Orders == nil {
		payload.Orders = []dto.OrderRecord{}
	}

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := s.cfg.HistoryPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, s.cfg.HistoryPath)
}

func mergeOrders(current, incoming dto.OrderRecord) dto.OrderRecord {
	result := current
	if shouldPreferIncoming(current, incoming) {
		result = incoming
	} else {
		result = current
	}

	result.DocumentID = firstNonEmpty(current.DocumentID, incoming.DocumentID)
	result.OrderName = firstNonEmpty(result.OrderName, current.OrderName, incoming.OrderName)
	result.Status = firstNonEmpty(result.Status, current.Status, incoming.Status)
	result.Filename = firstNonEmpty(result.Filename, current.Filename, incoming.Filename)
	result.Simpl = firstNonEmpty(result.Simpl, current.Simpl, incoming.Simpl)
	result.FullName = firstNonEmpty(result.FullName, current.FullName, incoming.FullName)
	result.GTIN = firstNonEmpty(result.GTIN, current.GTIN, incoming.GTIN)
	result.CreatedAt = earliestTimestamp(current.CreatedAt, incoming.CreatedAt)
	result.CreatedBy = firstNonEmpty(current.CreatedBy, incoming.CreatedBy)
	result.UpdatedAt = latestTimestamp(current.UpdatedAt, incoming.UpdatedAt)
	if shouldPreferIncoming(current, incoming) {
		result.UpdatedBy = firstNonEmpty(incoming.UpdatedBy, current.UpdatedBy)
	} else {
		result.UpdatedBy = firstNonEmpty(current.UpdatedBy, incoming.UpdatedBy)
	}
	result.TSDCreated = current.TSDCreated || incoming.TSDCreated
	result.TSDCreatedAt = latestTimestamp(current.TSDCreatedAt, incoming.TSDCreatedAt)
	if incoming.TSDIntroNumber != "" && parseTime(incoming.TSDCreatedAt).After(parseTime(current.TSDCreatedAt)) {
		result.TSDIntroNumber = incoming.TSDIntroNumber
		result.TSDCreatedBy = firstNonEmpty(incoming.TSDCreatedBy, current.TSDCreatedBy)
	} else {
		result.TSDIntroNumber = firstNonEmpty(current.TSDIntroNumber, incoming.TSDIntroNumber)
		result.TSDCreatedBy = firstNonEmpty(current.TSDCreatedBy, incoming.TSDCreatedBy)
	}
	if len(result.Positions) == 0 {
		if len(current.Positions) != 0 {
			result.Positions = current.Positions
		} else {
			result.Positions = incoming.Positions
		}
	}
	if len(result.Extra) == 0 {
		if len(current.Extra) != 0 {
			result.Extra = current.Extra
		} else {
			result.Extra = incoming.Extra
		}
	}
	return result
}

func shouldPreferIncoming(current, incoming dto.OrderRecord) bool {
	currentUpdated := parseTime(current.UpdatedAt)
	incomingUpdated := parseTime(incoming.UpdatedAt)
	if incomingUpdated.IsZero() {
		return false
	}
	if currentUpdated.IsZero() {
		return true
	}
	return incomingUpdated.After(currentUpdated) || incomingUpdated.Equal(currentUpdated)
}

func sortOrders(orders []dto.OrderRecord) {
	sort.Slice(orders, func(i, j int) bool {
		left := parseTime(orders[i].UpdatedAt)
		right := parseTime(orders[j].UpdatedAt)
		if left.Equal(right) {
			return orders[i].DocumentID < orders[j].DocumentID
		}
		return left.After(right)
	})
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err == nil {
		return parsed
	}
	parsed, err = time.Parse("2006-01-02T15:04:05", value)
	if err == nil {
		return parsed
	}
	return time.Time{}
}

func latestTimestamp(left, right string) string {
	leftTime := parseTime(left)
	rightTime := parseTime(right)
	if leftTime.IsZero() {
		return right
	}
	if rightTime.IsZero() {
		return left
	}
	if rightTime.After(leftTime) {
		return right
	}
	return left
}

func earliestTimestamp(left, right string) string {
	leftTime := parseTime(left)
	rightTime := parseTime(right)
	if leftTime.IsZero() {
		return right
	}
	if rightTime.IsZero() {
		return left
	}
	if rightTime.Before(leftTime) {
		return right
	}
	return left
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func nowISO() string {
	return time.Now().Format(time.RFC3339Nano)
}

func currentUser() string {
	if value := os.Getenv("USERNAME"); value != "" {
		return value
	}
	if value := os.Getenv("USER"); value != "" {
		return value
	}
	return "unknown"
}
