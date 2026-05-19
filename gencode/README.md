# A generator cmd to generate server api and resource
gencode will generate files: api service resource

## Usage
```
go install github.com/linkingthing/gorest/gencode@latest

#specify package
gencode user -pkg github.com/acme/myproject

#specify package name and folder
gencode user -f ./internal -pkg github.com/acme/myproject

#only generate service file
gencode user -f . -only service

this will gen files:
api/user_api.go
service/user_service.go
resource/user_resource.go
```

