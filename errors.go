/*
 * Copyright 2022, Cloudchacho
 * All rights reserved.
 */

package taskhawk

import "errors"

// ErrTaskNotFound indicates that task was not found
var (
	ErrTaskNotFound = errors.New("task not found")
)
