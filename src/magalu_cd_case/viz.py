
import matplotlib.pyplot as plt

def bar_times(capitals, recife_h, salvador_h, out_path):
    plt.figure(figsize=(10,5))
    x = range(len(capitals))
    width = 0.4
    plt.bar([i - width/2 for i in x], recife_h, width, label="Recife")
    plt.bar([i + width/2 for i in x], salvador_h, width, label="Salvador")
    plt.xticks(list(x), capitals, rotation=30, ha="right")
    plt.ylabel("Tempo de viagem (h)")
    plt.title("Tempos rodoviários (ilustrativos)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()

def bar_coverage(labels, values, out_path, title):
    plt.figure(figsize=(7,4))
    plt.bar(labels, values)
    plt.ylabel("Cobertura (fração da demanda)")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()
