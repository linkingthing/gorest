package generator

const resourceTpl = `package resource

import (
	restdb "github.com/linkingthing/gorest/db"
	restresource "github.com/linkingthing/gorest/resource"
)

type {{.Pascal}} struct {
	restresource.ResourceBase ` + "`" + `json:",inline"` + "`" + `
	// TODO: 添加业务字段
}

var Table{{.Pascal}} = restdb.ResourceDBType(&{{.Pascal}}{})
`

const serviceTpl = `package service

import (
	restresource "github.com/linkingthing/gorest/resource"

	"{{.PkgBase}}/resource"
)

type {{.Pascal}}Service struct {
}

func New{{.Pascal}}Service() *{{.Pascal}}Service {
	return &{{.Pascal}}Service{}
}

func (s *{{.Pascal}}Service) Create{{.Pascal}}(r *resource.{{.Pascal}}) error {
	// TODO: 实现创建逻辑
	return nil
}

func (s *{{.Pascal}}Service) Update{{.Pascal}}(r *resource.{{.Pascal}}) error {
	// TODO: 实现更新逻辑
	return nil
}

func (s *{{.Pascal}}Service) Delete{{.Pascal}}(ctx *restresource.Context) error {
	// TODO: 实现删除逻辑
	return nil
}

func (s *{{.Pascal}}Service) Get{{.Pascal}}(ctx *restresource.Context) (*resource.{{.Pascal}}, error) {
	// TODO: 实现查询逻辑
	return nil, nil
}

func (s *{{.Pascal}}Service) List{{.Pascal}}(ctx *restresource.Context) ([]*resource.{{.Pascal}}, error) {
	// TODO: 实现查询逻辑
	return nil, nil
}

`

const apiTpl = `package api

import (
	goresterr "github.com/linkingthing/gorest/error"
	restresource "github.com/linkingthing/gorest/resource"

	"{{.PkgBase}}/resource"
	"{{.PkgBase}}/service"
)

type {{.Pascal}}Api struct {
	svc *service.{{.Pascal}}Service
}

func New{{.Pascal}}Api() *{{.Pascal}}Api {
	return &{{.Pascal}}Api{svc: service.New{{.Pascal}}Service()}
}

func (h *{{.Pascal}}Api) Create(ctx *restresource.Context) (restresource.Resource, *goresterr.APIError) {
	// TODO: 解析请求体，增加其他业务调用 
	r := ctx.Resource.(*resource.{{.Pascal}})
	if err := h.svc.Create{{.Pascal}}(r); err != nil {
		return nil, goresterr.NewAPIError(goresterr.ServerError, err)
	}
	
	return r, nil
}

func (h *{{.Pascal}}Api) Update(ctx *restresource.Context) (restresource.Resource, *goresterr.APIError) {
	// TODO: 解析请求体，增加其他业务调用 
	r := ctx.Resource.(*resource.{{.Pascal}})
	if err := h.svc.Update{{.Pascal}}(r); err != nil {
		return nil, goresterr.NewAPIError(goresterr.ServerError, err)
	}
	
	return r, nil
}

func (h *{{.Pascal}}Api) Delete(ctx *restresource.Context) *goresterr.APIError {
	// TODO: 解析请求体，增加其他业务调用 
	if err := h.svc.Delete{{.Pascal}}(ctx); err != nil {
		return goresterr.NewAPIError(goresterr.ServerError, err)
	}
	
	return nil
}

func (h *{{.Pascal}}Api) List(ctx *restresource.Context) (any, *goresterr.APIError) {
	// TODO: 解析请求体，增加其他业务调用
	rs, err := h.svc.List{{.Pascal}}(ctx)
	if err != nil {
		return nil, goresterr.NewAPIError(goresterr.ServerError, err)
	}
	
	return rs, nil
}

func (h *{{.Pascal}}Api) Get(ctx *restresource.Context) (restresource.Resource, *goresterr.APIError) {
	// TODO: 解析请求体，增加其他业务调用
	r, err := h.svc.Get{{.Pascal}}(ctx)
	if err != nil {
		return nil, goresterr.NewAPIError(goresterr.ServerError, err)
	}
	
	return r, nil
}
`
