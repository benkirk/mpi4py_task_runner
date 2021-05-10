default: run

clean:
	rm -f output-*.tar

clobber:
	$(MAKE) clean
	rm -f *~ *.pyc

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


output.tar: $(wildcard output-?????.tar)
	echo "Combining $?"
	echo " into $@"
	rm -f output.tar
	mv output-00001.tar tmp-out.tar
	for file in output-?????.tar; do \
	  echo $$file ; \
	  tar --concatenate --file=tmp-out.tar $$file ; \
	  rm -f $$file ; \
	done
	mv tmp-out.tar output.tar

summary:
	for file in out*.tar; do \
	  tar xf $$file --wildcards "*/summary.txt" --to-command=cat; \
	done

byte_summary:
	for file in out*.tar; do \
	  tar xf $$file --wildcards "*/summary.txt" --to-command="cat | grep \"bytes\""; \
	done

testtree: Makefile
	rm -rf testtree testdir.tmp
	for a in $$(seq 1 16); do \
	  dirname=$$(printf "testdir.tmp/%02d/" $$a) ; \
	  echo $$dirname && mkdir -p $$dirname && echo $$dirname > $$dirname/out1.txt &&  date > $$dirname/out1.txt ; \
	  for b in $$(seq 1 16); do \
	    dirname=$$(printf "testdir.tmp/%02d/%02d/" $$a $$b) ; \
	    echo $$dirname && mkdir -p $$dirname && echo $$dirname > $$dirname/out2.txt && date > $$dirname/out2.txt ; \
	    for c in $$(seq 1 16); do \
	      dirname=$$(printf "testdir.tmp/%02d/%02d/%02d/" $$a $$b $$c) ; \
	      echo $$dirname && mkdir -p $$dirname && echo $$dirname > $$dirname/out3.txt && date >> $$dirname/out3.txt ; \
	    done ; \
	  done ; \
	done
	mv testdir.tmp testtree && find testtree -type f
