import string
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import requests
import matplotlib.pyplot as plt


# Функція для отримання тексту з URL
def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        return None


# Функція для видалення знаків пунктуації
def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))


# Функція Map
def map_function(word):
    return word, 1


# Функція Shuffle
def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


# Функція Reduce
def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


# Виконання MapReduce
def map_reduce(text):
    # Видалення знаків пунктуації
    text = remove_punctuation(text).lower()

    # Розбивка тексту на слова
    words = text.split()

    # Паралельний Map
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Паралельна Reduce
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)


# Візуалізація топ-10 слів
def visualize_top_words(word_counts, top_n):
    # Сортуємо слова за частотою
    sorted_word_counts = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
    top_words = sorted_word_counts[:top_n]

    # Розпаковуємо слова і частоти
    words, frequencies = zip(*top_words)

    # Побудова графіка
    plt.figure(figsize=(10, 6))
    plt.barh(words, frequencies, color="skyblue")
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title("Top 10 Most Frequent Words")
    plt.gca().invert_yaxis()  # Найчастіше слово зверху
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Вхідний текст для обробки
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = get_text(url)

    # Перевірка наявності тексту
    if text:
        # Підрахунок всіх слів, без фільтрації
        result = map_reduce(text)

        # Візуалізація топ-10
        visualize_top_words(result, top_n=10)
    else:
        # Обробка помилки
        print("Помилка: Не вдалося отримати вхідний текст.")
