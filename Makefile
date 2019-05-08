default: run

clean:
	rm -f output-*.tar

run:
	mpirun-mpich-mp -n 25 ./run.py

serial:
	for cnt in $$(seq 1 10); do \
	  stepdir=$$(printf "step_%05d" $$cnt) ; \
	  echo $$stepdir && mkdir -p $$stepdir && cd $$stepdir; \
	  ../write_rand_data.py && cd - >/dev/null 2>&1; \
	done
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
