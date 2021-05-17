.PHONY: build

build:
	env CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -o ./prediction_set ./main.go

run:
	POSTGRES_USER=grover POSTGRES_PASS=pass1 go run ./main.go

deploy: build
	aws s3 cp ./prediction_set s3://dev-misty/prediction_set
	aws s3 presign s3://dev-misty/prediction_set