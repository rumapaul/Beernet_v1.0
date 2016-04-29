all: drawgraph logviewer

logviewer: LogViewer.oz
	ozc -c LogViewer.oz -o LogViewer.ozf

drawgraph: DrawGraph.oz
	ozc -c DrawGraph.oz -o DrawGraph.ozf

clean:
	rm -f *.oz~ *.ozf

.PHONY: clean all

