
class Point():
    def __init__(self, components=[], dimensions=[0, 0], num_points=1):
        # features of the point
        self.components = components
        # Dimension of point
        self.x_y_dimensions = dimensions
        # how many points are in this point
        self.num_points = num_points

    def point(self):
        self.x_y_dimensions = [0, 0]
    
    def add_coord(self, coord):
        self.components.append(coord)
        self.num_points += 1
    
    def get_num_points(self):
        return self.num_points

    def getDimensions(self):
        return self.x_y_dimensions
    
    def add_to_average(self, coord):
        # print(self.x_y_dimensions)
        new_average_x = (self.num_points * self.x_y_dimensions[0] + coord[0]) / (self.num_points + 1)
        new_average_y = (self.num_points * self.x_y_dimensions[1] + coord[1]) / (self.num_points + 1)
        
        self.num_points += 1
        self.x_y_dimensions = [new_average_x, new_average_y]
        # print(self.x_y_dimensions)

    def average(self):
        x = float(0)
        y = float(0)
        count = 0
        for x_y_coord in self.components:
            # print(self.components)
            x += x_y_coord[0]
            y += x_y_coord[1]
            count += 1
        if count > 0:
            new_centroid = [x/count, y/count]
            return new_centroid
        return [10000, 10000]

        """
        def sum(self, p):
        for i in range(p):
            self.components[i] += p.components[i]
        self.num_points += p.numPoints
        for i in range(len(self.x_y_dimensions)):
            temp = self.components[i] / self.num_points;
            self.components[i] = (float)Math.round(temp*100000)/100000.0f;
        }
        """
        self.numPoints = 1

"""
public static Point copy(final Point p) {
    Point ret = new Point(p.components);
    ret.numPoints = p.numPoints;
    return ret;
}

public void set(final float[] c) {
    this.components = c;
    this.dim = c.length;
    this.numPoints = 1;
}

    def distance(self, p, h):
    if h < 0:
        # Consider only metric distances
        h = 2;       
    if h == 0:
        # Chebyshev formula
        max = -1
        float diff = 0.0f;
        for (int i = 0; i < this.dim; i++) {
            diff = Math.abs(this.components[i] - p.components[i]);
            if (diff > max) {
                max = diff;
            }                       
        }
        return max;

    } else {
        // Manhattan, Euclidean, Minkowsky
        float dist = 0.0f;
        for (int i = 0; i < this.dim; i++) {
            dist += Math.pow(Math.abs(this.components[i] - p.components[i]), h);
        }
        dist = (float)Math.round(Math.pow(dist, 1f/h)*100000)/100000.0f;
        return dist;
    }
}

"""