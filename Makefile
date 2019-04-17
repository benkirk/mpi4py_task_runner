default: run

clean:
	rm -f output-*.tar

run:
	mpirun-mpich-mp -n 25 ./run.py

list:
	for file in out*.tar; do \
	  echo $$file ":" ; \
	  tar tvf $$file ; \
	done

extract:
	for file in out*.tar; do \
	  echo $$file ":" ; \
	  tar xvf $$file ; \
	done
