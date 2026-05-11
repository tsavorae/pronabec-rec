#define N 2

int wg = 0;
bool done = false;

chan out = [N] of { int };

proctype worker(int id)
{
    out!id;

    atomic {
        wg--;

        if
        :: wg == 0 ->
            done = true
        :: else ->
            skip
        fi
    }
}

proctype main()
{
    int i;

    atomic {
        wg = N
    }

    i = 0;
    do
    :: i < N ->
        run worker(i);
        i++
    :: else ->
        break
    od;

    i = 0;
    do
    :: i < N ->
        out?_;
        i++
    :: else ->
        break
    od;

    do
    :: done -> break
    :: else -> skip
    od
}

init {
    run main()
}

ltl no_negative {
    [] (wg >= 0)
}

ltl eventually_done {
    <> (done)
}

ltl all_done {
    <> (wg == 0 && done)
}
