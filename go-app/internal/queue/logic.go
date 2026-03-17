package queue

import "github.com/kirillbelykh/kontur_api/go-app/internal/dto"

var downloadedStatuses = map[string]struct{}{
	"Скачан":     {},
	"Downloaded": {},
}

var tsdReadyStatuses = map[string]struct{}{
	"Скачан":        {},
	"Downloaded":    {},
	"Ожидает":       {},
	"Скачивается":   {},
	"Готов для ТСД": {},
}

func IsOrderReadyForIntro(item dto.OrderRecord) bool {
	if item.DocumentID == "" {
		return false
	}
	_, ok := downloadedStatuses[item.Status]
	return ok || item.Filename != ""
}

func IsOrderReadyForTSD(item dto.OrderRecord) bool {
	if item.DocumentID == "" {
		return false
	}
	_, ok := tsdReadyStatuses[item.Status]
	return ok || item.Filename != ""
}

func RemoveOrderByDocumentID(downloadList []dto.OrderRecord, documentID string) ([]dto.OrderRecord, bool) {
	if documentID == "" {
		return downloadList, false
	}

	remaining := make([]dto.OrderRecord, 0, len(downloadList))
	removed := false
	for _, item := range downloadList {
		if item.DocumentID == documentID {
			removed = true
			continue
		}
		remaining = append(remaining, item)
	}
	return remaining, removed
}
