
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
    
    def sum(self, p):
        for i in range(p):
            self.components[i] += p.components[i]
        self.num_points += p.numPoints


    def average(self):
        """
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