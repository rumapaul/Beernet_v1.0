INSTALL = install
DESTDIR = ../../lib/java-api/beernet
MAKE = make
JAVA_SRC = BadSecretException BeernetHandler DebugHandler DhtAPI DhtUtils \
		   DhtUtilsAPI NoSetException  Transaction SecretProtectedException \
		   ValueNotFoundException SetAlreadyExistException

all: $(foreach jsource, $(JAVA_SRC), jsrc_$(jsource))

install: check_destdir $(foreach source, $(SRCS), install_$(source))

check_destdir:
	if ! test -d $(DESTDIR); then mkdir $(DESTDIR); fi

install_%:
	$(INSTALL_FUNCTOR) $(subst install_,,$@).ozf $(DESTDIR)

jsrc_%:
	javac $(subst jsrc_,,$@).java

clean:
	rm -f *class

.PHONY: all clean

