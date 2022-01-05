import matplotlib.pyplot as plt
from random import randint
import numpy as np
import random

def draw_step(x = [], y = []):
    plt.title('plt.step(where=...)')
    plt.step(x, y, label='pre (default)')
    plt.show()
    
def draw_gannt(x_pairs, stage = 0, xlim = [480,3960]):
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
        start = f'{j[0]}'
        value = f'{j[1]}'
        ax.text(x=j[0] -200 , y=startY, s=start)
        ax.text(x=j[0] + j[1], y=startY, s= value)
        startY += 10

    ax.set_title(f'Tasks at stage {stage}')
    ax.set_ylim(0, 220)
    ax.set_xlim(xlim[0], xlim[1])
    ax.set_xlabel('mintues since start')
    ax.set_yticks([15 + 10*idx for idx, j in enumerate(x_pairs)])
    ax.set_yticklabels(['job '+str(idx) for idx, j in enumerate(x_pairs)])

    ax.grid(True)

    plt.show()