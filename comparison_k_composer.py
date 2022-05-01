from unittest import result
from mrjob.job import MRJob
from k_clustering_point_class import Point
import math
import random
#       MapReduce
# class MRCountSum(MRJob):

#       Python without MapReduce
import csv

file = open('python_preprocess.csv')

csvreader = csv.reader(file)

header = []
header = next(csvreader)
rows = []
for row in csvreader:
    rows.append(row)
rows
file.close()

# 1 get coords
# 2 get centroids
# 3 assign every object to the cluster it is most similar to
# NEW: Use Euclidean distance to calculate how similar these
#       objects are
#       therefore turn everything to a number
# 4 NEW everytime a new memeber joins a cluster recalculate
#   the cluster representatives
#   Categorize each data items to its closest centroid and update th
#   centroid coordinates calculating the average of items coordinates
#   categorized in that group so far

# SO we update the center running, instead of iteratively

def mapper(centroids, coords):
    c_dict = {}
    for c in centroids:
        c_dict[centroids.index(c)] = []
    """
        # closest_centroid = -1
        # for c in centroids:
        #     closest_centroid = centroids.index(c)
        #     print(closest_centroid)
    """
    for coord in coords:
        min_dist = math.inf
        for c in centroids:
            distance = getDistance(c, coord)
            if distance < min_dist:
                closest_centroid = centroids.index(c)
                min_dist = distance
        list_i = c_dict[closest_centroid]
        list_i.append(coord)
    return c_dict 
        
        
        # c.add_coord(coord)
        # c.add_to_average(coord)

def new_mapper(centroids, coords):
    list_0 = []
    list_1 = []
    list_2 = []
    list_3 = []
    list_4 = []
    """
    n0 = 0
    n1 = 0
    n2 = 0
    n3 = 0
    n4 = 0
    """
    # print(len(coords))
    for coord in coords:
        min_dist = math.inf
        closest_centroid = -1
        for c in centroids:
            distance = getDistance(c, coord)
            if distance < min_dist:
                closest_centroid = centroids.index(c)
                min_dist = distance
        if closest_centroid == 0:
            string = " ; " + str(coord) + " ; " + str(1)
            list_0.append(string)
            # n0 += 1
            # print(len(list_0))
        elif closest_centroid == 1:
            string = " ; " + str(coord) + " ; " + str(1)
            list_1.append(string)
            # print(len(list_1))
            # n1 += 1
        elif closest_centroid == 2:
            string = " ; " + str(coord) + " ; " + str(1)
            list_2.append(string)
            # print(len(list_2))
            # n2 += 1
        elif closest_centroid == 3:
            string = " ; " + str(coord) + " ; " + str(1)
            list_3.append(string)
            # print(len(list_3))
            # n3 += 1
        elif closest_centroid == 4:
            string = " ; " + str(coord) + " ; " + str(1)
            list_4.append(string)
            # n4 += 1
        # c.add_coord(coord)
    """
    print(n0)
    print(n1)
    print(n2)
    print(n3)
    print(n4)
    """
    total_list = [list_0, list_1, list_2, list_3, list_4]
    return total_list

def getDistance(c, coord):
    cur_dist = math.sqrt(pow(coord[0]-c[0], 2) + pow(coord[1] - c[1], 2))
    return cur_dist

def getRandomCentroids(coords, k=3):
    centroids = []
    print(len(coords))
    point_index = []
    i_list = [784724, 1108647, 1458107, 495697, 1378509]
    # i_list = [1284724, 1008647, 58107, 1405697, 8509]
    for i in range(k):
        # value = random.randint(0,len(coords)-1)
        value = i_list[i]
        print("VALUE: " + str(value))
        point_index.append(coords[value])
    # print(point_index)
    for i in range(k):
        p = Point([], point_index[i])
        centroids.append(p)
    return centroids

def getCentroids(list):
    centroids = []
    for coord in list:
        p = Point([], coord)
        centroids.append(p)
    return centroids

def getCoords():
    allcoord = []
    for line in rows:
        # line[-1] = ''.join(c for c in line[-1] if c not in '()')
        # coords = line[-1].split(',')

        try:
            x_coord = float(line[-2].strip())
        except ValueError:
            x_coord = float(0)
        
        try:
            y_coord = float(line[-1].strip())
        except ValueError:
            y_coord = float(0)
        new_coord = [x_coord, y_coord]
        allcoord.append(new_coord)
    return allcoord

"""
def reducer(centroids):
    new_centroids = []
    for c in centroids:
        print(c.getDimensions())
        new_mean = c.average()
        new_centroids.append([new_mean[0], new_mean[1]])
    return new_centroids
"""

def reducer_variation():
    centroid_variation = []
    for c in centroids:
        variation = c.variation()
        centroid_variation.append(variation)
    return centroid_variation

def changeFormat(values):
    new_v_list = []
    for v in values:
        final_value = ";  " + str(v) + " ; 1 "
        new_v_list.append(final_value)
    return new_v_list 

def get_int_coord(v):
    v_info = ''.join(c for c in str(v) if c not in '[ "]')
    coord_num = v_info.split(';')
    # num = int(coord_num[2])
    # num_points += num

    coords = coord_num[1].split(',')
    try:
        x_coord = float(coords[0].strip())
    except ValueError:
        x_coord = float(0)

    try:
        y_coord = float(coords[1].strip())
    except ValueError:
        y_coord = float(0)
    new_coord = [x_coord, y_coord]
    return new_coord

def composer(key, values, centroids):
    final_value = centroids[(key-1)]
    # dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
    num_points = 0
    final_x = 0
    final_y = 0
    
    for v in values:
        coord = get_int_coord(v)
        num_points += 1
        final_x += coord[0]
        final_y += coord[1]
    final_value = [final_x, final_y]
    final_value = ";  " + str(final_value) + " ; " + str(num_points)
    return key, final_value

def reducer_WOC(key, values, centroids):
    final_value = centroids[(key-1)]
    num_points = 1
    final_x = final_value[0]
    final_y = final_value[1]
    
    for v in values:
        num_points += 1
        final_x += v[0]
        final_y += v[1]
    final_value = [final_x, final_y]
    new_average_x = (final_x) / (num_points)
    new_average_y = (final_y) / (num_points)
    final_value = [new_average_x, new_average_y]
    final_value = ";  " + str(final_value) + " ; " + str(num_points)
    return key, final_value

def reducer_WOC_ASINFILE(key, values, centroids):
    final_value = centroids[(key-1)]
    num_points = 0
    for v in values:
        num_points += 1
        new_average_x = (num_points * final_value[0] + v[0]) / (num_points + 1)
        new_average_y = (num_points * final_value[1] + v[1]) / (num_points + 1)
        final_value = [new_average_x, new_average_y]
    final_value = ";  " + str(final_value) + " ; " + str(num_points)
    return key, final_value

def getWeightAndCoord(v):
    v_info = ''.join(c for c in str(v) if c not in '[ )"]')
    coord_num = v_info.split(';')
    list_weight = int(coord_num[2])

    coords = coord_num[1].split(',')
    try:
        x_coord = float(coords[0].strip())
    except ValueError:
        x_coord = float(0)

    try:
        y_coord = float(coords[1].strip())
    except ValueError:
        y_coord = float(0)
    
    new_coord = [x_coord, y_coord]
    return list_weight, new_coord

def reducer_COMP(key, values, centroids):
    final_value = centroids[(key-1)]
    num_points = 1
    for v in values: #[2:]:
        list_weight, new_coord = getWeightAndCoord(v)
        num_points += list_weight
        final_value[0] += new_coord[0]
        final_value[1] += new_coord[1]

    new_average_x = (final_value[0]) / (num_points)
    new_average_y = (final_value[1]) / (num_points)
    final_value = [new_average_x, new_average_y]
    final_value = ";  " + str(final_value) + " ; " + str(num_points)
    return key, final_value




if __name__ == "__main__":
    k=5
    done = False
    # coords = getCoords()
    centroids=[[41.98131263, -87.806945473],
    [41.771488695, -87.667641182],
    [41.884494554, -87.627138636],
    [41.754594962, -87.70872738],
    [41.840581183804865, -87.67204270608761]]

    dict_i = {}
    dict_i[0] = [[41.975821967, -87.807053208], [41.896808547, -87.762472408], [41.931561031, -87.74444156], [41.924495568, -87.802606325], [41.947941831, -87.748305001], [41.916908245, -87.747934514], [41.975289354, -87.751291037], [41.935693883, -87.74445132], [41.939991899, -87.728199012], [41.921278418, -87.775776358], [41.942917209, -87.727153183], [41.9094083, -87.754439499], [41.912521423, -87.759591647], [41.896588269, -87.771223557], [41.928244258, -87.748933342], [41.928561566, -87.764578066], [41.940127372, -87.746902545], [41.933187127, -87.751196211], [41.98967395, -87.791552412], [42.021869039, -87.67410027], [41.99876544, -87.683672172], [41.940176841, -87.727362915], [41.938460477, -87.766897531], [41.936123388, -87.723689228], [41.911811442, -87.771402295], [41.931044901, -87.719768573], [41.88810604, -87.77408172], [41.936123388, -87.723689228], [41.962838891, -87.713543913], [41.973017672, -87.713640143]]
    dict_i[1] = [[41.691784636, -87.635115968], [41.687020002, -87.60844523], [41.729712374, -87.653158513], [41.724330486, -87.638434248], [41.778539988, -87.674035599], [41.771736514, -87.66400571], [41.766012869, -87.58288376], [41.772186514, -87.638570496], [41.789249657, -87.621841243], [41.753507587, -87.661904591], [41.779814746, -87.600442358], [41.748448075, -87.679202816], [41.8002858, -87.69642307], [41.657827784, -87.544575368], [41.782951712, -87.604362828], [41.774967846, -87.605113138], [41.785980536, -87.653451443], [41.72913814, -87.586745763], [41.72256088, -87.666300236], [41.757374833, -87.631231515], [41.727730234, -87.656751235], [41.71529807, -87.532859401], [41.77487569, -87.636719434], [41.759496235, -87.618336201], [41.694892728, -87.635364292], [41.744169485, -87.610034343], [41.652924602, -87.545915891], [41.784100806, -87.602083852], [41.785143676, -87.612074654], [41.754823635, -87.551092573]]
    dict_i[2] = [[41.911824751, 0.0], [41.87799981, -87.64308039], [41.966409353, -87.648852157], [41.852159338, -87.633007505], [41.968654176, -87.659532372], [41.830907597, -87.616958635], [41.964488675, -87.662378087], [41.931215751, -87.669573467], [41.897564713, -87.676026728], [41.933129385, -87.6792964], [41.93574044, -87.698598034], [41.895969036, -87.637160727], [41.882748442, -87.632387132], [41.884594218, -87.624607293], [41.852793105, -87.628049053], [42.001678618, -87.660604177], [41.905486973, -87.684651776], [41.95054562, -87.704105067], [41.880652933, -87.634367274], [41.949518457, -87.646963262], [41.939464159, -87.678048161], [41.892655759, -87.616922817], [41.882249994, -87.629410745], [41.879258911, -87.651262835], [41.908196281, -87.671795783], [41.978257946, -87.65517969], [41.951899593, -87.646364227], [41.881635267, -87.62450045], [41.884603381, -87.624313404], [41.902466926, -87.633023725]]
    dict_i[3] = [[41.783163561, -87.727772951], [0.0, -87.706364915], [41.791518681, -87.729098793], [41.745267715, -87.699825354], [41.765923753, -87.689395237], [41.776925486, -87.716371088], [41.690745749, -87.706745901], [41.799536176, -87.744166558], [41.811804589, -87.746596204], [41.797864355, -87.764752309], [41.805201777, -87.733301934], [41.779054245, -87.697597443], [41.788364783, -87.727922015], [41.778822808, -87.719120967], [41.761372382, -87.693087732], [41.780353674, -87.695865069], [41.774818809, -87.702896431], [41.793240933, -87.730967524], [41.776990398, -87.69806877], [41.796967099, -87.704736791], [41.784095038, -87.717153014], [41.778098976, -87.753576153], [41.808871048, -87.749307968], [41.808871048, -87.749307968], [41.79582565, -87.723091675], [41.778997352, -87.708233997], [41.754592961, -87.741528537], [41.772486481, -87.739527446], [41.782061296, -87.789978129], [41.789240349, -87.741642671]]
    dict_i[4] = [[41.817229156, -87.637328162], [41.869772159, -87.708180162], [41.831494381, -87.667977499], [41.834522994, -87.682642417], [41.921794873, -87.713268849], [41.843787854, -87.709893838], [41.88171118, -87.753455712], [41.921325664, -87.723010704], [41.902041291, -87.731205967], [41.905359186, -87.727574721], [41.884673766, -87.711780717], [41.826313153, -87.67272382], [41.91824807, -87.712527912], [41.853450045, -87.728299687], [41.827600477, -87.648865183], [41.894928829, -87.755903553], [41.822419341, -87.702059168], [41.889243518, -87.750622111], [41.908494665, -87.722688418], [41.847391284, -87.703894742], [41.902032672, -87.69259546], [41.860327455, -87.730966058], [41.869201889, -87.670589881], [41.848942976, -87.713701284], [41.828216148, -87.619270835], [41.808722626, -87.664237066], [41.882223021, -87.703696217], [41.8962257, -87.735829834], [41.877111469, -87.723714312], [41.811614799, -87.701501325]]

    a1List = []
    a2List = []
    for i in range(5):
        list_i = dict_i[i]
        a1 = reducer_WOC(i, list_i, centroids)
        a2 = reducer_WOC_ASINFILE(i, list_i, centroids)
        a1List.append(a1)
        a2List.append(a2)
    """
    print('--------------------')
    print(a1List)
    print(a2List)
    """
    
    to_c = []
    a3List = []
    for i in range(5):
        # for j in range(10):
        list_i = dict_i[i]
        for_list = changeFormat(list_i)
        l1 = for_list[0:5]
        l2 = for_list[5:15]
        l3 = for_list[15:23]
        l4 = for_list[23:31]
        to_c = [l1, l2, l3, l4]
        # total_len = len(l1) + len(l2) + len(l3) + len(l4)  

        composer_result = []
        for k in range(len(to_c)):
            comp_part_ans = composer(i, to_c[k], centroids)
            # print(len(to_c[k]))
            composer_result.append(comp_part_ans[1])
        
        ans3 = reducer_COMP(i, composer_result, centroids)
        a3List.append(ans3)
    
    """
    for i in range(5):
        print(a1List[i])
        print(a2List[i])
        print(a3List[i])
    """
    
    


    """
    useful_coords = coords[0:1000]
    to_composer_dict = mapper(centroids, coords)

    for i in range(5):
        list_i = to_composer_dict[i]
        useful_num = list_i[0:30]
        to_composer_dict[i] = useful_num
    
    
    for i in range(5):
        list_i = to_composer_dict[i]
        print(i)
        print(list_i)
    """

    """
    to_comp = []
    to_comp_2 = []
    for l in to_composer_list:
        print(len(l))
        to_comp.append(l[0:10])
        to_comp_2.append(l[10:20])


    ans_list = []
    ans_list_2 = []
    for i in range(len(to_comp)):
        ans = combiner(i+1, to_comp[i], centroids)
        ans_list.append(ans)
        ans_2 = combiner(i+1, to_comp_2[i], centroids)
        ans_list_2.append(ans_2)
    print(len(ans_list))
    print()
    print()
    """

    """
    ans_list = [(1, ';  [41.94064576118182, -87.76540692872727] ; 10'), (2, ';  [41.74868812018182, -87.64054877636363] ; 10'), (3, ';  [41.91080437300001, -79.6832585797273] ; 10'), (4, ';  [37.97430409336364, -87.72180151727271] ; 10'), (5, ';  [41.869965447436805, -87.70241643064433] ; 10')]
    ans_list_2 = [(1, ';  [41.944037376636366, -87.75423783681818] ; 10'), (2, ';  [41.75553082245455, -87.63049906518181] ; 10'), (3, ';  [41.911292945090906, -87.65260294827273] ; 10'), (4, ';  [41.780889260909085, -87.71020836872727] ; 10'), (5, ';  [41.86484948470954, -87.70740072882614] ; 10')]

    final_v = []
    for i in range(len(ans_list)):
        value=[ans_list[i][1], ans_list_2[i][1]]
        final = reducer_2(i+1, value, centroids)
        final_v.append(final)
    print(ans_list)
    print(ans_list_2)
    print(final_v)
    """

    """
    i = 1
    while done == False:
        centroids = getCentroids(mean)
        mapper(centroids, coords, k)
        new_mean = reducer(centroids)
        i += 1
        if new_mean == mean:
            done = True
        else:
            mean = new_mean
    """
