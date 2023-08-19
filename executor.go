package migrate

/*
Executor 拥有多个处理程序，可以对外输出处理程序列表
*/

type Executor interface {
	ListHandlers() ([]Handler, error)
}
