VERSION = 0.7.1
SPEC = dist/rpm/foxing.spec

srpm:
	dnf install -y cargo curl
	spectool -g -R $(SPEC) --define "_sourcedir $(CURDIR)"
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
	    sh -s -- -y --default-toolchain nightly --profile minimal --no-modify-path
	export PATH="$$HOME/.cargo/bin:$$PATH" && \
	    cargo vendor > /dev/null && \
	    tar czf foxing-$(VERSION)-vendor.tar.gz vendor/
	rpmbuild -bs $(SPEC) \
	    --define "_sourcedir $(CURDIR)" \
	    --define "_srcrpmdir $(outdir)"
