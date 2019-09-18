"""
Author: Kyler Little
"""


import psycopg2
import os
import itertools as it
import copy


"""
Attributes in the relation
"""
attrs = ['pname', 'discount', 'month', 'price']


"""
List of tuple pairs are of the form: ([], [])
==> list of attributes in tuple[1] functionally
    depend on list of attributes in tuple[0] 
"""
functional_deps = []


def computeAllSubsets(S):
    """Return powerset of S - [] where S is a simple list
    """
    # Since it won't include [], it's not quite a power set
    almost_power_set = []

    # Get each combination of length i (from 1 to len(attrs))
    for i in range(len(S)):
        almost_power_set.append(it.combinations(S, i+1))

    # Convert the innermost items from tuples to lists.
    L = []
    for x in almost_power_set:
        L_x = list(x)
        L.append([list(y) for y in L_x])
    return L


def isFunctionalDep(A, B, cur):
    """
    Determine if functional dependency exists between A and B
    (i.e. A ==> B)

    @params: A is a list, B is a list
    """
    subquery = " AND ".join(['S3.{0} = S1.{0}'.format(x) for x in A])
    _A = ",".join(A)
    _B = ",".join(B)

    query = 'SELECT {a} FROM mysales as S1 GROUP BY {a} HAVING 1  \
        < (SELECT COUNT(*) FROM (SELECT DISTINCT {b} FROM mysales \
        as S3 WHERE {sq}) as S2);'.format(a=_A, b=_B, sq=subquery)
    cur.execute(query)
    res = cur.fetchall()

    # If res is empty, functional dep; else, not
    if not res:
        global functional_deps
        functional_deps.append((A, B))
        print("Query: {q}".format(q=query))
    return len(res) == 0


def isTrivialDep(A, B):
    """
    Determine if A ==> B is a trivial functional dependency.
    A ==> B is trivial if B is a subset of A

    @params: A is a list, B is a list
    """
    return set(B).issubset(set(A))


def computeClosure(X, func_deps):
    """Compute {X}^+ set (closure set) of list of attributes X
    """
    prev = set()
    closure = set(X)

    # True flag implies a change so we must reiterate through func_deps
    flag = True

    # iterate while no change
    while flag == True:
        cp = copy.deepcopy(func_deps)
        for fd in cp:
            # unpack tuple
            X, Y = fd

            # if X is in the closure set
            if set(X).issubset(closure):
                prev = copy.deepcopy(closure)

                # add Y to the set
                closure = closure.union(set(Y))

                # if we actually updated closure, must re-enter for loop
                if closure != prev:
                    flag = True
                    break
        else:
            # if we don't break, set flag to False
            flag = False
    
    return list(closure)

def computeFDsOfDecomposedRelation(R1, fd):
    T = []
    T_subsets = computeAllSubsets(R1)
    for attrSet in T_subsets:
        for X in attrSet:
            X_plus = computeClosure(X, fd)
            for B in X_plus:
                if B in R1 and B not in X:
                    T.append((X, [B]))
    
    return list(filter(lambda tup: not isTrivialDep(tup[0], tup[1]), T))


def decomposeToBCNF(R, func_deps):
    # Any relation with 2 attributes is in BCNF
    if len(R) <= 2:
        return R

    for dep in func_deps:
        X, Y = dep

        # 1. Expand R to include X_plus (i.e. closure of X)
        closure = computeClosure(X, func_deps)

        # if X ==> Y is nontrivial and X is NOT a key, dep violates BCNF
        if not isTrivialDep(X, Y) and len(closure) != len(R):
            # 2. Decompose R into R1(X+) and R2(X, R - X+)
            R1 = closure
            R2 = list(set(X).union(set(R).difference(set(closure))))

            # 3. Find the FD's for the decomposed relations
            R1_fds = computeFDsOfDecomposedRelation(R1, func_deps)
            R2_fds = computeFDsOfDecomposedRelation(R2, func_deps)
            print("R1 {r} has FDs: {fd}".format(r=R1, fd=R1_fds))
            print("R2 {r} has FDs: {fd}".format(r=R2, fd=R2_fds))

            # 4. Compute keys for R1, R2
            R1_keys = computeMinimalKeys(R1, R1_fds)
            R2_keys = computeMinimalKeys(R2, R2_fds)
            print("Key(s) for {0}: {1}".format(R1, R1_keys))
            print("Key(s) for {0}: {1}".format(R2, R2_keys))

            # 5. Iterate over all resulting subschemas until all in BCNF
            res1 = decomposeToBCNF(R1, R1_fds)
            res2 = decomposeToBCNF(R2, R2_fds)
            if res1 and res2:
                return [res1, res2]
            elif res1:
                return [res1]
            else:
                return [res2]
    else:
        # done when no functional dep is violated
        return R

def computeMinimalKeys(R, func_deps):
    keyset = []
    minkeylen = len(R)

    allSubsets = computeAllSubsets(R)
    for attrSet in allSubsets:
        for attrCombo in attrSet:
            closureOfSubset = computeClosure(attrCombo, func_deps)
            if set(closureOfSubset) == set(R):
                # Test if min key
                if len(attrCombo) < minkeylen:
                    keyset = [attrCombo]
                    minkeylen = len(attrCombo)
                elif len(attrCombo) == minkeylen:
                    keyset.append(attrCombo)
    
    return keyset

def main():
    # Initialize local database client connection; open cursor to perform db operations
    conn = psycopg2.connect("dbname={db} user={usr} password={pw}"
        .format(db='hw5', usr=os.environ.get('USER'), pw='12345'))
    cur = conn.cursor()


    # Problem #1
    #   b.) Find all functional dependencies in the relation.
    L = computeAllSubsets(attrs)
    # Run queries to determine functional dependencies
    for attrSet in L:
        for attrSet2 in copy.deepcopy(L):
            for attrCombo in attrSet:
                for attrCombo2 in [a for a in attrSet2 if set(a) != set(attrCombo)]:
                    # If not a trivial dep and a functional dep
                    if not isTrivialDep(attrCombo, attrCombo2) and \
                    isFunctionalDep(attrCombo, attrCombo2, cur):
                        print("{1} ==> {0}\n".format(attrCombo2, attrCombo))
                        if len(attrCombo2) == len(attrs):
                            print("KEY ALERT: {0} is a key for the relation.".format(attrCombo))

    # Problem #1
    #   c.) Decompose the table to BCNF relations.
    global functional_deps
    res = decomposeToBCNF(attrs, functional_deps)
    print("Decomposed relations: {r}".format(r=res))
    

    print("\nAll functional dependencies: {fd}".format(fd=functional_deps))

    print("Problem #2, Relation #1")
    fd = [(['A'], ['E']),
          (['B', 'C'], ['A']),
          (['D', 'E'], ['B'])
         ]
    R = ['A', 'B', 'C', 'D', 'E']
    keyset = computeMinimalKeys(R, fd)
    print("Minimal key(s): {mk}".format(mk=keyset))
    decomposition = decomposeToBCNF(R, fd)
    print("Decomposition: {0}".format(decomposition))

    
    print("Problem #2, Relation #2")
    fd = [(['C'], ['D']),
          (['C'], ['A']),
          (['B'], ['C'])
         ]
    S = ['A', 'B', 'C', 'D']
    keyset = computeMinimalKeys(S, fd)
    print("Minimal key(s): {mk}".format(mk=keyset))
    decomposition = decomposeToBCNF(S, fd)
    print("Decomposition: {0}".format(decomposition))


    print("TEST -- BCNF decomposition")
    fd = [(['D'], ['B']),
          (['C', 'E'], ['A'])
         ]
    S = ['A', 'B', 'C', 'D', 'E']
    res = decomposeToBCNF(S, fd)
    print("TEST -- Decomposed relations: {r}".format(r=res))


    # Close communication channel with database
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
