MAKE = make
INSTALL = install
INSTALL_FUNCTOR = $(INSTALL) -m 644
DESTDIR = ../../lib/utils/
SRCS = HashedList KeyRanges PbeerList PbeerQueue PbeerIdList Random RingList TextFile Misc

all: $(foreach source, $(SRCS), src_$(source)) nanoStore

install: check_destdir $(foreach source, $(SRCS), install_$(source))

check_destdir:
	if ! test -d $(DESTDIR); then mkdir $(DESTDIR); fi

install_%:
	$(INSTALL_FUNCTOR) $(subst install_,,$@).ozf $(DESTDIR)

src_%:
	ozc -c $(subst src_,,$@).oz -o $(subst src_,,$@).ozf

nanoStore: nanoStore.oz
	ozc -c nanoStore.oz -o nanoStore

clean:
	rm -f *ozf
	rm -f nanoStore
	rm -f nano.tket

.PHONY: all clean
