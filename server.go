package gorest

import (
	"net/http"

	goresterr "github.com/zdnscloud/gorest/error"
	"github.com/zdnscloud/gorest/resource"
)

type HandlerFunc func(*resource.Context) *goresterr.APIError
type HandlersChain []HandlerFunc

type EndHandlerFunc func(*resource.Context, *goresterr.APIError) *goresterr.APIError
type EndHandlersChain []EndHandlerFunc

type Server struct {
	Schemas     resource.SchemaManager
	handlers    HandlersChain
	endHandlers EndHandlersChain
}

func NewAPIServer(schemas resource.SchemaManager) *Server {
	return &Server{
		Schemas: schemas,
	}
}

func (s *Server) Use(h HandlerFunc) {
	s.handlers = append(s.handlers, h)
}

func (s *Server) EndUse(h EndHandlerFunc) {
	s.endHandlers = append(s.endHandlers, h)
}

func (s *Server) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ctx, err := resource.NewContext(rw, req, s.Schemas)
	if err != nil {
		WriteResponse(rw, err.Status, err)
		return
	}

	for _, h := range s.handlers {
		if err := h(ctx); err != nil {
			WriteResponse(rw, err.Status, err)
			return
		}
	}

	err = restHandler(ctx)
	if err != nil {
		WriteResponse(rw, err.Status, err)
	}

	for _, h := range s.endHandlers {
		if err := h(ctx, err); err != nil {
			return
		}
	}
}
