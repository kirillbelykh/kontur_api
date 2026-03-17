package introduction

import (
	"time"
)

func DefaultProductionWindow(reference time.Time) (string, string) {
	production := shiftMonth(reference, -2)
	productionDate := time.Date(production.Year(), production.Month(), 1, 0, 0, 0, 0, reference.Location())
	expirationDate := time.Date(productionDate.Year()+5, productionDate.Month(), 1, 0, 0, 0, 0, reference.Location())
	return productionDate.Format("02-01-2006"), expirationDate.Format("02-01-2006")
}

func shiftMonth(reference time.Time, delta int) time.Time {
	year, month, _ := reference.Date()
	monthIndex := int(month) - 1 + delta
	shiftedYear := year + monthIndex/12
	shiftedMonth := monthIndex % 12
	if shiftedMonth < 0 {
		shiftedMonth += 12
		shiftedYear--
	}
	return time.Date(shiftedYear, time.Month(shiftedMonth+1), 1, 0, 0, 0, 0, reference.Location())
}
