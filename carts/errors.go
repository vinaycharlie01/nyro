package cart

const errNotFoundMsg = "value not found in cart"

// NotFound is returned when a key is not present in the cart.
type NotFound struct {
	cause error
}

// NotFoundWithCause creates a NotFound error wrapping the given cause.
func NotFoundWithCause(e error) error {
	return &NotFound{cause: e}
}

// Cause returns the underlying cause error.
func (e NotFound) Cause() error {
	return e.cause
}

// Is reports whether the target error matches this NotFound error.
func (e NotFound) Is(err error) bool {
	return err.Error() == errNotFoundMsg
}

// Error implements the error interface.
func (e NotFound) Error() string {
	return errNotFoundMsg
}

// Unwrap returns the cause for errors.Is/As chaining.
func (e NotFound) Unwrap() error { return e.cause }
