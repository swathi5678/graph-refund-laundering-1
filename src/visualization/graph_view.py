import networkx as nx
import matplotlib.pyplot as plt

def draw(G):
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True)
    plt.show()
