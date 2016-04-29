MAKE = make
INSTALL = install
INSTALL_FUNCTOR = $(INSTALL) -m 644
DESTDIR = ../../lib/trappist/
BINDIR = ../../bin/
SRCS = SymmetricReplication Trappist
SUBDIRS = twophase paxos eagerpaxos valueset

all: $(foreach source, $(SRCS), src_$(source)) sublibs

install: check_destdir \
	$(foreach source, $(SRCS), install_$(source)) \
	installlibs install_test

sublibs:$(foreach subdir, $(SUBDIRS), sub_$(subdir))

installlibs:$(foreach subdir, $(SUBDIRS), subinstall_$(subdir))

cleanlibs:$(foreach subdir, $(SUBDIRS), subclean_$(subdir))

check_destdir:
	if ! test -d $(DESTDIR); then mkdir $(DESTDIR); fi

install_%:
	$(INSTALL_FUNCTOR) $(subst install_,,$@).ozf $(DESTDIR)

install_test:
	if ! test -d $(BINDIR); then mkdir $(BINDIR); fi
	$(INSTALL) test $(BINDIR)

src_%:
	ozc -c $(subst src_,,$@).oz -o $(subst src_,,$@).ozf

sub_%:
	$(MAKE) -C $(subst sub_,,$@) all

subinstall_%:
	$(MAKE) -C $(subst subinstall_,,$@) install

tester: Tester.oz
	ozc -c Tester.oz -o tester

clean: cleanlibs
	rm -rf *ozf
	rm -rf tester

subclean_%:
	$(MAKE) -C $(subst subclean_,,$@) clean

.PHONY: all clean

