package go_pool

type options[T any] struct {
	size   int
	taskCB func(t T, i int)
	doneCB func(t T, i int)
	debug  bool
}

type Option[T any] interface {
	apply(*options[T])
}

// 连接池大小
type sizeOption[T any] int

func (s sizeOption[T]) apply(o *options[T]) {
	o.size = int(s)
}

func WithSize[T any](size int) Option[T] {
	return sizeOption[T](size)
}

// 连接池任务回调
type taskCBOption[T any] func(t T, i int)

func (t taskCBOption[T]) apply(o *options[T]) {
	o.taskCB = t
}

func WithTaskCB[T any](taskCB func(t T, i int)) Option[T] {
	return taskCBOption[T](taskCB)
}

// 连接池事后回调
type doneCBOption[T any] func(t T, i int)

func (e doneCBOption[T]) apply(o *options[T]) {
	o.doneCB = e
}

func WithDoneCB[T any](exitCB func(t T, i int)) Option[T] {
	return doneCBOption[T](exitCB)
}

// 调试日志
type debugOption[T any] bool

func (d debugOption[T]) apply(o *options[T]) {
	o.debug = bool(d)
}

func WithDebug[T any](debug bool) Option[T] {
	return debugOption[T](debug)
}
