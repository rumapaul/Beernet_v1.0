MAKE 	= make
SUBDIRS	= src tools docsrc java-api

all: lib bin java-api

help:
	@echo "Beernet's main Makefile" 
	@echo "To build and install Beernet and its tools, run:"
	@echo ""
	@echo "make"
	@echo "make install"
	@echo ""
	@echo "Warning: documentation needs to be build and installed separately."
	@echo ""
	@echo "To build each part independently run:"
	@echo ""
	@echo "make doc\tto build documentation"
	@echo "make lib\tto build Beernet components"
	@echo "make bin\tto build Beernet tools"
	@echo "make java-api\tto build Java interfacing tools"
	@echo ""
	@echo "To install each part independently run:"
	@echo ""
	@echo "make install-doc\t to install documentation"
	@echo "make install-lib\t to install Beernet components"
	@echo "make install-bin\t to install Beernet tools"
	@echo "make install-java-api\t to install Java interfacing tools"
	@echo ""
	@echo "To clean directories docsrc, src, tools, and java, run:"
	@echo ""
	@echo "make clean"
	@echo ""
	@echo "To clean everything, including directories bin, lib, and doc, run:"
	@echo ""
	@echo "make veryclean"
	@echo ""
	@echo "a beer a day keeps the doctor away"	
	@echo "Beernet is released under the Beerware License (see file LICENSE)" 

doc: 
	$(MAKE) -C docsrc all

lib: 
	$(MAKE) -C src all

bin: 
	$(MAKE) -C tools all

java-api:
	$(MAKE) -C java-api/oz-interface all

install: install-lib install-bin

install-doc:
	$(MAKE) -C docsrc install

install-lib:
	$(MAKE) -C src install

install-bin:
	$(MAKE) -C tools install

install-java-api:
	$(MAKE) -C java-api/oz-interface install

clean: cleanlibs

cleanlibs:$(foreach subdir, $(SUBDIRS), subclean_$(subdir))

subclean_%:
	$(MAKE) -C $(subst subclean_,,$@) clean

veryclean: clean
	rm -rf bin/*
	rm -rf doc/*
	rm -rf lib/*

.PHONY: all clean doc lib bin java-api
