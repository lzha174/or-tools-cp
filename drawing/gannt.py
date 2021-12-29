import matplotlib.pyplot as plt
from random import randint
import random


def draw_gannt(x_pairs):
    no_of_colors = len(x_pairs)
    colors = ["#" + ''.join([random.choice('0123456789ABCDEF') for i in range(6)])
             for j in range(no_of_colors)]

    fig, ax = plt.subplots()
    startY = 10


    for j in x_pairs:
        #https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.broken_barh.html#matplotlib.axes.Axes.broken_barh
        # Axes.broken_barh(xranges, yrange, *, data=None, **kwargs)[source]
        #  xrange ssequence of tuples (xmin, xwidth), yrange(ymin, yheight)
        color = randint(0, no_of_colors - 1)
        ax.broken_barh([j], (startY, 10), facecolors=colors[color])
        startY += 10

    ax.set_ylim(0, 220)
    ax.set_xlim(0, 120)
    ax.set_xlabel('seconds since start')
    ax.set_yticks([15 + 10*idx for idx, j in enumerate(x_pairs)])
    ax.set_yticklabels(['job '+str(idx) for idx, j in enumerate(x_pairs)])
    ax.grid(True)

    plt.show()