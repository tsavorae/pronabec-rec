#define N 2 
int wg = 0;             
int done = 0;            

chan out = [N] of { int }; 

inline lock() {
    atomic { wg >= 0 -> skip }
}

inline unlock() {
    atomic { wg >= 0 -> skip }
}

proctype worker(int id) {
    atomic {
        wg = wg - 1
    }
    
    out!id;
    
    atomic {
        wg = wg - 1;
        if
            :: wg == 0 -> done = 1
            :: else -> skip
        fi
    }
}


proctype main() {
    int i;
    int count = 0;
    
    wg = N;
    
    i = 0;
    do
    :: i < N ->
        run worker(i);
        i = i + 1
    :: else -> break
    od;
    
    i = 0;
    do
    :: i < N ->
        out?_;
        i = i + 1
    :: else -> break
    od;
    
    do
    :: done == 0 -> skip
    :: else -> break
    od
}

init {
    run main()
}


ltl no_deadlock {
    <> (done == 1)
}

ltl no_negative {
    [] (wg >= 0)
}

ltl all_done {
    <> (wg == 0 && done == 1)
}
