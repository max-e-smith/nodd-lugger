package internal

type Request interface {
	Resolve()
	VerifyTarget()
	Download()
}

func Submit(request Request) {
	request.Resolve()
	request.VerifyTarget()
	request.Download()
}
