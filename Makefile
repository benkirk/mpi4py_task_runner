default: run

clean:
	rm -f output-*.tar

run:
	mpirun-mpich-mp -n 25 ./run.py
