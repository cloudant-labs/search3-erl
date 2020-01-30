
version = 3008005fec8272a28bc8fbc8700d4242bf1cb2cf
url = https://raw.githubusercontent.com/cloudant-labs/search3-java/$(version)/src/main/proto/search3.proto

all: update compile


update:
	wget $(url) -O protos/search3.proto.tmp || exit 1
	ls -l .
	ls -l protos/
	cmp protos/search3.proto protos/search3.proto.tmp; \
	if [ "$$?" != "0" ]; then \
		cp protos/search3.proto.tmp protos/search3.proto ;\
	fi
	$(MAKE) generate


generate: src/search_client.erl src/search3_pb.erl


src/search_client.erl src/search3_pb.erl: protos/search3.proto
	REBAR_COLOR=none rebar3 grpc gen


compile:
	rebar compile


clean:
	@rm -rf _build
	@rm -f src/search3_pb.erl
	@rm -f src/search_client.erl
