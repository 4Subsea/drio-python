

runtests:
	python -m unittest discover


coverage:
	coverage3 run --source=./timeseriesclient -m unittest discover -s timeseriesclient/test && coverage3 report --omit=*/test/* -m 
 
clean:
	rm -r timeseriesclient/*.pyc
	rm -r timeseriesclient/*/*.pyc
