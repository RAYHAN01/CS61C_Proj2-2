from pyspark import SparkContext
import Sliding, argparse

def bfs_map(value):
    """ YOUR CODE HERE """
    # This function is the argument of flatMap method, and it should return a list.
    # format of value: (pos, level) pair
    rtn = [value] 
    # Should not change the pos in previous level, the rtn list contains value itself
    if (level <= value[1]): 
    # Check if the value pair exceeds the level. If it is larger than level, we need
    # append rtn with all of it's children.
        child = Sliding.children(WIDTH, HEIGHT, Sliding.hash_to_board(WIDTH, HEIGHT, value[0]))
        for _ in child:
            rtn.append((Sliding.board_to_hash(WIDTH, HEIGHT, _), value[1]+1))
    return rtn

def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    return min(value1, value2)
    # This function is the argument of reduceByKey method.
    # bfs_reduce: exclude the identical pos in rdd by only leaving one with smaller level.

def exchange(value):
	return (value[1], value[0])
    # This function is the argument of map method.
    # After we find out all the solution, we need switch back from (pos, level) to (level, pos).

def solve_puzzle(master, output, height, width, slaves):
    global HEIGHT, WIDTH, level
    HEIGHT = height
    WIDTH = width
    level = 0

    sc = SparkContext(master, "python")
    sol = Sliding.board_to_hash(WIDTH, HEIGHT, Sliding.solution(WIDTH, HEIGHT))
    """ YOUR CODE HERE """
    previous = 0 # num of element in previous rdd
    curr = 1
    # The increment of curr from previous is the condition of while loop.
    # If it is larger than 0, it means we find new (pos, level), and we keep looping.
    # Otherwise, the while loop ends since there is no new (pos, level) pair.
    pos_to_level = [(sol, 0)]
    rdd = sc.parallelize(pos_to_level).partitionBy(PARTITION_COUNT)
    # intitialization of rdd and partition by 16, a number we find can boost up our speed.
    while curr - previous > 0:
    	rdd = rdd.flatMap(bfs_map, True) \
                 .flatMap(bfs_map, True).partitionBy(PARTITION_COUNT).reduceByKey(bfs_reduce, PARTITION_COUNT) \
                 .flatMap(bfs_map, True) \
                 .flatMap(bfs_map, True).partitionBy(PARTITION_COUNT).reduceByKey(bfs_reduce, PARTITION_COUNT)
        previous = curr
        curr = rdd.count()
        level += 4
        # Since we do 4 flatMap, level increases by 4 every time.
    	
    level_to_pos = rdd.map(exchange, True).sortByKey(True)
    # Exchange rdd(pos_to_level) to a new rdd contains level_to_pos.
    # Then the new one is SortByKey, so that (level, pos) pair is in order as level 
    # increasing from the first pos,(0, sol), to final pos.
    # Finally, we collect() to make a list and assign it to level_to_pos
    # We use a while loop to do the output job.
    level_to_pos.coalesce(slaves).saveAsTextFile(output)

    sc.stop()



""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    parser.add_argument("-S", "--slaves", type=int, default=6,
            help="number of slaves executing the job")
    args = parser.parse_args()

    global PARTITION_COUNT
    PARTITION_COUNT = args.slaves * 16

    # call the puzzle solver
    solve_puzzle(args.master, args.output, args.height, args.width, args.slaves)

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
