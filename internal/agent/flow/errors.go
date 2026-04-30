package flow

import "errors"

// ErrSkipReply 表示 Graph 节点希望本轮静默不回复。
// Bot 会用 errors.Is 识别并直接吞掉它。
var ErrSkipReply = errors.New("skip reply")
