# TODO: figure out how augmented these heaps need to be
#       Is current version good enough, or does there need to be a mapping from uuid to node instance for updating?

class MaxHeap:
  def __init__(self, collection=None):
    # Initialize a heap using list of elements (<queue_size>, <uuid>)
    self.heap = []
    if collection is not None:
      self.heap = collection

  def get_parent_position(self, i):
    # The parent is located at floor((i-1)/2)
    return int((i - 1) / 2)

  def get_left_child_position(self, i):
    # The left child is located at 2 * i + 1
    return 2 * i + 1

  def get_right_child_position(self, i):
    # The right child is located at 2 * i + 2
    return 2 * i + 2

  def has_parent(self, i):
    # This function checks if the given node has a parent or not
    return self.get_parent_position(i) < len(self.heap)

  def has_left_child(self, i):
    # This function checks if the given node has a left child or not
    return self.get_left_child_position(i) < len(self.heap)

  def has_right_child(self, i):
    # This function checks if the given node has a right child or not
    return self.get_right_child_position(i) < len(self.heap)

  def insert(self, key):
    self.heap.append(key)  # Adds the key to the end of the list
    self.heapify(len(self.heap) - 1)  # Re-arranges the heap to maintain the heap property

  def get_max(self):
    return self.heap[0]  # Returns the largest value in the heap in O(1) time.

  def heapify(self, i):
    # Loops until it reaches a leaf node
    while (self.has_parent(i) and self.heap[i][0] > self.heap[self.get_parent_position(i)][0]):
      # Swap the values
      self.heap[i], self.heap[self.get_parent_position(i)] = self.heap[self.get_parent_position(i)], self.heap[i]
      i = self.get_parent_position(i)  # Resets the new position

  def print_heap(self):
    print(self.heap)  # Prints the heap


class MinHeap:
  def __init__(self, collection=None):
    # Initialize a heap using list of elements (<queue_size>, <uuid>)
    self.heap = []
    if collection is not None:
      self.heap = collection

  def get_parent_position(self, i):
    # The parent is located at floor((i-1)/2)
    return int((i - 1) / 2)

  def get_left_child_position(self, i):
    # The left child is located at 2 * i + 1
    return 2 * i + 1

  def get_right_child_position(self, i):
    # The right child is located at 2 * i + 2
    return 2 * i + 2

  def has_parent(self, i):
    # This function checks if the given node has a parent or not
    return self.get_parent_position(i) < len(self.heap)

  def has_left_child(self, i):
    # This function checks if the given node has a left child or not
    return self.get_left_child_position(i) < len(self.heap)

  def has_right_child(self, i):
    # This function checks if the given node has a right child or not
    return self.get_right_child_position(i) < len(self.heap)

  def insert(self, key):
    self.heap.append(key)  # Adds the key to the end of the list
    self.heapify(len(self.heap) - 1)  # Re-arranges the heap to maintain the heap property

  def get_min(self):
    return self.heap[0]  # Returns the largest value in the heap in O(1) time.

  def heapify(self, i):
    # Loops until it reaches a leaf node
    while (self.has_parent(i) and self.heap[i][0] < self.heap[self.get_parent_position(i)][0]):
      # Swap the values
      self.heap[i], self.heap[self.get_parent_position(i)] = self.heap[self.get_parent_position(i)], self.heap[i]
      i = self.get_parent_position(i)  # Resets the new position

  def print_heap(self):
    print(self.heap)  # Prints the heap