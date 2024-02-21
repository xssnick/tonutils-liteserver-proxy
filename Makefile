proxy:
	echo "If you will get an error, make sure to compile the library first: compile-lib-linux"
	CGO_ENABLED=1 go build -o build/liteserver cmd/main.go

lib-linux:
	mkdir -p build/lib
	rm -rf ton-build
	mkdir ton-build
	cd ton-build ; cmake -DCMAKE_BUILD_TYPE=Release ../ton ; cmake --build emulator -j$(nproc) ; cp emulator/libemulator.so ../internal/emulate/lib/ ; cp emulator/emulator_export.h ../internal/emulate/lib/
	cp internal/emulate/lib/libemulator.so ./build/lib/
	echo "Done! Run make build now"