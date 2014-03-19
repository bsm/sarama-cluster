default: test

.DEFAULT:
	cd cluster && $(MAKE) $@
