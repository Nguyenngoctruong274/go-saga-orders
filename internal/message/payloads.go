package message

import (
	"errors"
	"fmt"
	"regexp"
)

// The commands of the order saga, as participants know them.
const (
	InventoryReserve = "inventory.reserve"
	InventoryRelease = "inventory.release"
	PaymentAuthorize = "payment.authorize"
	PaymentVoid      = "payment.void"
)

// Item is one order line.
type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// ReservePayload asks inventoryd to hold stock for an order.
type ReservePayload struct {
	OrderID string `json:"order_id"`
	Items   []Item `json:"items"`
}

// Validate implements Payload.
func (p ReservePayload) Validate() error {
	var errs []error
	if p.OrderID == "" {
		errs = append(errs, errors.New("order_id is required"))
	}
	if len(p.Items) == 0 {
		errs = append(errs, errors.New("at least one item is required"))
	}
	for i, it := range p.Items {
		if it.SKU == "" {
			errs = append(errs, fmt.Errorf("item %d: sku is required", i))
		}
		if it.Quantity < 1 {
			errs = append(errs, fmt.Errorf("item %d: quantity must be at least 1, got %d", i, it.Quantity))
		}
	}
	return errors.Join(errs...)
}

// ReleasePayload undoes the reservation of an order. Releasing an order
// with no reservation is not an error: the compensation may run after a
// reserve whose outcome was never known.
type ReleasePayload struct {
	OrderID string `json:"order_id"`
}

// Validate implements Payload.
func (p ReleasePayload) Validate() error { return requireOrder(p.OrderID) }

// AuthorizePayload asks paymentd to hold an amount on the customer's
// payment method. Amounts are integers in the currency's minor unit
// (cents, or dong for VND, which has none): never floats.
type AuthorizePayload struct {
	OrderID     string `json:"order_id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

var currencyCode = regexp.MustCompile(`^[A-Z]{3}$`)

// Validate implements Payload.
func (p AuthorizePayload) Validate() error {
	var errs []error
	if err := requireOrder(p.OrderID); err != nil {
		errs = append(errs, err)
	}
	if p.AmountMinor < 1 {
		errs = append(errs, fmt.Errorf("amount_minor must be positive, got %d", p.AmountMinor))
	}
	if !currencyCode.MatchString(p.Currency) {
		errs = append(errs, fmt.Errorf("currency %q is not an ISO 4217 code", p.Currency))
	}
	return errors.Join(errs...)
}

// VoidPayload releases the authorization of an order; like a release, a
// void of nothing succeeds.
type VoidPayload struct {
	OrderID string `json:"order_id"`
}

// Validate implements Payload.
func (p VoidPayload) Validate() error { return requireOrder(p.OrderID) }

// ResultPayload is the optional body of a successful reply: the
// participant's own reference for what it did, for the timeline.
type ResultPayload struct {
	Reference string `json:"reference"`
}

// Validate implements Payload.
func (p ResultPayload) Validate() error {
	if p.Reference == "" {
		return errors.New("reference is required")
	}
	return nil
}

func requireOrder(id string) error {
	if id == "" {
		return errors.New("order_id is required")
	}
	return nil
}
