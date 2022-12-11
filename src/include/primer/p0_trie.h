//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rwlatch.h"

namespace bustub {

template <typename T>
class TrieNodeWithValue;

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) {
    LOG_DEBUG("TrieNode: Create a new Trie Node with given key %c.", key_char);
    this->key_char_ = key_char;
    this->is_end_ = false;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    if (this != &other_trie_node) {
      LOG_DEBUG("TireNode: Create a Trie Node by calling Move constructor with key %c.", other_trie_node.GetKeyChar());
      key_char_ = other_trie_node.key_char_;
      is_end_ = other_trie_node.is_end_;
      if (!children_.empty()) {
        children_.clear();
      }
      for (auto iter = other_trie_node.children_.begin(); iter != other_trie_node.children_.end(); iter++) {
        children_[iter->first] = std::move(iter->second);
      }
    }
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { return children_.count(key_char) > 0; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const {
    for (auto iter = children_.begin(); iter != children_.end(); iter++) {
      if (iter->second != nullptr) return true;
    }
    return false;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    LOG_DEBUG("TireNode: Insert a child node with key %c, and child's key %c.", key_char, child->GetKeyChar());
    if (HasChild(key_char)) return nullptr;
    if (key_char != child->key_char_) return nullptr;
    children_[key_char] = std::move(child);
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    if (HasChild(key_char)) {
      return &children_[key_char];
    }
    return nullptr;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    if (HasChild(key_char)) {
      LOG_DEBUG("TrieNode: Remove child node with key %c.", key_char);
      children_.erase(key_char);
    }
  }

  /**
   * @brief Update child node from TrieNode type to TrieNodeWithValue type.
   *
   * @param key_char Key char of child node to be removed
   * @param value Value to be inserted
   * @return True if update succeeds, false if value already exists
   */
  template <typename T>
  bool UpdateChildNode(char key_char, T value) {
    LOG_DEBUG("TrieNode: update child node with key %c.", key_char);
    if (is_end_) return false;
    if (!HasChild(key_char)) return false;
    std::unique_ptr<TrieNode> child = std::move(children_[key_char]);
    std::unique_ptr<TrieNodeWithValue<T>> new_child(new TrieNodeWithValue<T>(key_char, value));
    children_[key_char] = std::move(new_child);
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode)) {
    LOG_DEBUG("TrieNodeWithValue: Create a new terminal trie node from a TrieNode, with key %c.", key_char_);
    value_ = value;
    is_end_ = true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char) {
    LOG_DEBUG("TrieNodeWithValue: Create a new terminal trie node, with key %c.", key_char);
    value_ = value;
    is_end_ = true;
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  // Trie() = default;
  Trie() { root_ = std::make_unique<TrieNode>('\0'); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    latch_.WLock();
    LOG_DEBUG("Trie: Insert key %s, its len %lu.", key.c_str(), key.length());
    if (key.length() == 0) {
      latch_.WUnlock();
      return false;
    }
    std::unique_ptr<TrieNode> *pre_node = nullptr;
    std::unique_ptr<TrieNode> *cur_node = &root_;
    bool not_exists = false;
    size_t key_len = key.length();
    for (size_t i = 0; i < key_len; i++) {
      char k = key[i];
      if (!(*cur_node)->HasChild(k)) {
        if (i < key_len - 1) {
          (*cur_node)->InsertChildNode(k, std::make_unique<TrieNode>(k));
        }
        not_exists = true;
      }
      pre_node = cur_node;
      cur_node = (*cur_node)->GetChildNode(k);
    }
    /** if not exists, cur_node is father node of the terminal node;
        if exists, cur_node is the terminal node.
    */
    char k = key[key_len - 1];
    // case 1. TrieNode with this ending character does not exist
    if (not_exists) {
      // std::unique_ptr<TrieNodeWithValue<T>> child(new TrieNodeWithValue<T>(k, value));
      // (*cur_node)->InsertChildNode(k, std::move(child));
      assert(pre_node != nullptr && cur_node == nullptr);
      (*pre_node)->InsertChildNode(k, std::make_unique<TrieNodeWithValue<T>>(k, value));
      LOG_DEBUG("Trie: Insert succeeds since there is no this key %s in TrieTree.", key.c_str());
      latch_.WUnlock();
      return true;
    }
    assert(pre_node != nullptr && cur_node != nullptr);
    // case 2. the terminal node is already a TrieNodeWithValue
    if ((*cur_node)->IsEndNode()) {
      LOG_DEBUG("Trie: Insert fails since there is this key %s in TrieTree.", key.c_str());
      latch_.WUnlock();
      return false;
    }
    // case 3. the terminal node is a TrieNode
    // (*pre_node)->RemoveChildNode(k);
    // std::unique_ptr<TrieNodeWithValue<T>> child(new TrieNodeWithValue<T>(std::move(*cur_node->get()), value));
    // (*pre_node)->InsertChildNode(k, std::move(child));
    (*pre_node)->template UpdateChildNode(k, value);
    LOG_DEBUG("Trie: Insert succeeds since the node of key %s is not a terminal node.", key.c_str());
    latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool Remove(const std::string &key) {
    latch_.WLock();
    LOG_DEBUG("Trie: Remove key %s.", key.c_str());
    size_t key_len = key.length();
    if (key_len == 0) {
      latch_.WUnlock();
      return false;
    }
    std::unique_ptr<TrieNode> *cur_node = &root_;
    bool success = false;
    Delete(cur_node, key, 0, key_len, success);
    std::string res = success ? "remove succeeds" : "remove fails";
    LOG_DEBUG("Trie: Remove key %s result: %s", key.c_str(), res.c_str());
    latch_.WUnlock();
    return success;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    latch_.RLock();
    LOG_DEBUG("Trie: GetValue key(%s).", key.c_str());
    *success = false;
    if (key.length() == 0) {
      latch_.RUnlock();
      return {};
    }
    std::unique_ptr<TrieNode> *cur_node = &root_;
    for (char k : key) {
      if (!(*cur_node)->HasChild(k)) {
        latch_.RUnlock();
        return {};
      }
      cur_node = (*cur_node)->GetChildNode(k);
    }
    assert(cur_node != nullptr);
    auto casted = dynamic_cast<TrieNodeWithValue<T> *>((*cur_node).get());
    if (casted == nullptr) {
      latch_.RUnlock();
      return {};
    }
    *success = true;
    T res = casted->GetValue();
    latch_.RUnlock();
    return res;
  }

 private:
  /**
   * @brief Delete key value pair from the trie.

   * @param p_node current pointer of node of recursive function
   * @param key Key used to traverse the trie and find correct node
   * @param depth current depth of recursive function
   * @param len length of key
   * @param success if we find the corresponding node

   * @return True if need to delete child node and check whether delete myself
   */
  bool Delete(std::unique_ptr<TrieNode> *p_node, const std::string &key, size_t depth, size_t len, bool &success) {
    success = false;
    if (p_node == nullptr) return false;
    LOG_DEBUG("Trie: Delete key %s at depth %lu, with current node key_char %c.", key.c_str(), depth,
              (*p_node)->GetKeyChar());
    assert(depth == 0 || (*p_node)->GetKeyChar() == key[depth - 1]);
    if (depth == len) {
      if ((*p_node)->IsEndNode()) {
        (*p_node)->SetEndNode(false);
        success = true;
      }
      if (!(*p_node)->HasChildren()) {
        *p_node = nullptr;
        return true;
      }
      return false;
    }
    char k = key[depth];
    if (!(*p_node)->HasChild(k)) return false;
    bool need_to_delete = Delete((*p_node)->GetChildNode(k), key, depth + 1, len, success);
    if (need_to_delete) {
      (*p_node)->RemoveChildNode(k);
      if (!(*p_node)->IsEndNode() && !(*p_node)->HasChildren()) {
        *p_node = nullptr;
        return true;
      }
    }
    return false;
  }
};
}  // namespace bustub
