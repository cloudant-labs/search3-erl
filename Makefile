compile: src/search_client.erl
	@rebar compile

src/search_client.erl:
	@rebar grpc gen

clean:
	@rm -rf _build
	@rm -f src/search3_pb.erl
	@rm -f src/search_client.erl
