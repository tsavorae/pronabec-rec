#define N 2

bool mutex = false;
int active_readers = 0;

proctype Reader(int id) {
	atomic { !mutex -> active_readers++ }
	assert(mutex == false);
	active_readers--
}

proctype Writer() {
	atomic { active_readers == 0 && !mutex -> mutex = true }
	assert(active_readers == 0);
	mutex = false
}

init {
	run Reader(1);
	run Reader(2);
	run Writer();
}
