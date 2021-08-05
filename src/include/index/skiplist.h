//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist.h
//
// Identification: src/include/index/skiplist.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <utility>
#include <atomic>

namespace peloton {
namespace index {

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define MAX_LEVEL 16
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>
template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipList {
  // TODO: Add your declarations here
  public:
    class SkipNode;

    using KeyValuePair = std::pair<KeyType, ValueType>;

    using AtomSkipNode = std::atomic<SkipNode*>;

  public:
    class SkipNode {
      public:
        KeyValuePair item;
        AtomSkipNode* forward;
        int level;
        std::atomic<bool> deleted;

        SkipNode(const KeyValuePair& p_item, int p_level):
          item{p_item},
          level(p_level) {
          
          deleted.store(false);
          forward = new AtomSkipNode[level + 1];
          for(int i = 0; i <= level; i++) forward[i].store(nullptr);
        }
    };

  private:
    enum ERROR {REDUPLICATE, CAS_FAILED, DELETED, NO_ERROR};

  public:
    bool unique_keys;

    int cur_level;
    
    SkipNode* head;
    SkipNode* tail;

    // Key comparator
    const KeyComparator key_cmp_obj;

    // Raw key eq checker
    const KeyEqualityChecker key_eq_obj;

    // Check whether values are equivalent
    const ValueEqualityChecker value_eq_obj;

  public:
    inline bool KeyCmpLess(const KeyType &key1, const KeyType &key2) const {
      return key_cmp_obj(key1, key2);
    }

    inline bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const {
        return key_eq_obj(key1, key2);
    }

    inline bool KeyCmpGreaterEqual(const KeyType &key1,
                                  const KeyType &key2) const {
      return !KeyCmpLess(key1, key2);
    }

    inline bool KeyCmpGreater(const KeyType &key1, const KeyType &key2) const {
      return KeyCmpLess(key2, key1);
    }

    inline bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) const {
      return !KeyCmpGreater(key1, key2);
    }

    inline bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) {
      return value_eq_obj(v1, v2);
    }

    inline bool ObjCmpEqual(const KeyType &key1, const KeyType &key2, const ValueType &v1, const ValueType &v2) {
      return KeyCmpEqual(key1, key2) && ValueCmpEqual(v1, v2);
    }

    inline bool KeyEqualValueNotEqual(const KeyType &key1, const KeyType &key2, const ValueType &v1, const ValueType &v2) {
      return KeyCmpEqual(key1, key2) && !ValueCmpEqual(v1, v2);
    }

  public:
  ///////////////////////////////////////////////////////////////////
  // Member Functinos
  ///////////////////////////////////////////////////////////////////
    class ForwardIterator;

    ForwardIterator Begin() {
      return ForwardIterator{this};
    }

    ForwardIterator Begin(const KeyType &start_key) {
      return ForwardIterator{this, start_key};
    }

    SkipList(
      bool unique_flag,
      KeyComparator p_key_cmp_obj = KeyComparator{},
      KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
      ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{}) : 
      unique_keys(unique_flag),
      cur_level(0),
      key_cmp_obj{p_key_cmp_obj},
      key_eq_obj{p_key_eq_obj},
      value_eq_obj{p_value_eq_obj} {
      KeyType dummy_key;
      ValueType dummy_value = nullptr;

      head = new SkipNode(std::make_pair(dummy_key, dummy_value),MAX_LEVEL);
      tail = new SkipNode(std::make_pair(dummy_key, dummy_value),MAX_LEVEL);

      for(int i = 0; i <= MAX_LEVEL; i++) {
        head->forward[i].store(tail);
      }
    }

    ~SkipList() {
      return;
    }

    inline bool IsTailOrNull(SkipNode* node) {
      return node == nullptr || node->forward[0].load() == nullptr;
    }    

    int RandomLevel () {
      int v = 1;

      while ((((double)std::rand() / RAND_MAX)) < 0.5 && 
            std::abs(v) < MAX_LEVEL) {

          v += 1;
      }
      return abs(v);
    }

    void ResetCurLevel() {
      int i;
      for(i = MAX_LEVEL; i>0; i--) {
        if(!IsTailOrNull(head->forward[i].load())) break;
      }
      cur_level = i;
    }

    void UpdateList(const KeyType &key, SkipNode* update[MAX_LEVEL]) {
      SkipNode* prev = head;
      SkipNode* x = head;
      for(int i = cur_level; i >= 0; i--) {
        while (!IsTailOrNull(x->forward[i].load()) && KeyCmpGreaterEqual(key, x->forward[i].load()->item.first)) {
          prev = x;
          x = x->forward[i].load();
        }
        update[i] = x->deleted.load() ? prev : x;
      }
    }
      
    void UpdateList(const KeyType &key, const ValueType &value, SkipNode* update[MAX_LEVEL]) {
      SkipNode* prev = head;
      SkipNode* x = head;
      for(int i = cur_level; i >= 0; i--) {
        while (!IsTailOrNull(x->forward[i].load()) && KeyCmpGreaterEqual(key, x->forward[i].load()->item.first)) {
          if(ObjCmpEqual(x->forward[i].load()->item.first, key, x->forward[i].load()->item.second, value)) break;
          prev = x;
          x = x->forward[i].load();
        }
        update[i] = x->deleted.load() ? prev : x;
      }
    }

    //return exact pointer or prev. No head return. Only Tail
    SkipNode* MoveTo(const KeyType &key) {
      SkipNode* x = head;
      for(int i = cur_level; i >= 0; i--) {
        while (!IsTailOrNull(x->forward[i].load()) & KeyCmpLessEqual(x->forward[i].load()->item.first, key)) {
          x = x->forward[i].load();
          if(KeyCmpEqual(x->item.first, key)) break;
        }
      }
      return x == head ? x->forward[0].load() : x; //
    }

    void GetValue(const KeyType &key, std::vector<ValueType> &result) {
      SkipNode* x = MoveTo(key);
      while(!IsTailOrNull(x) && KeyCmpEqual(x->item.first, key) == true) {
        result.push_back(x->item.second);
        x = x->forward[0].load();
      }
    }

    SkipNode* Find(const KeyType &key, const ValueType& value) {
      SkipNode* x = MoveTo(key);
      while(!IsTailOrNull(x)) { //follow right poiner
          if(ObjCmpEqual(x->item.first, key, x->item.second, value)) {
            return x;
          }
          else if(!IsTailOrNull(x->forward[0].load()) && KeyCmpLessEqual(x->forward[0].load()->item.first, key)){
            x = x->forward[0].load();
          }
          else {
            break;
          }
      }

      return nullptr;
    }

    SkipNode* ConditionalFind(const KeyType &key, 
                              const ValueType &value,
                              std::function<bool(const void *)> predicate,
                              bool *predicate_satisfied) {
      std::vector<ValueType> result;
      GetValue(key, result);

      *predicate_satisfied = false;
      for (auto it = begin (result); it != end (result); ++it) {
        if(predicate((*it)) == true) {
          *predicate_satisfied = true;
          break;
        }
      }

      return Find(key, value);
    } 


    bool CanInsert(const KeyType &key, const ValueType &value) {
      SkipNode* node_p = Find(key, value);
      if(node_p != nullptr) {
        return false;
      }
      else {
        return true;
      }
    }

    ERROR InstallNode(SkipNode* node, SkipNode* update[MAX_LEVEL], int i) {
      SkipNode* prev_node = update[i];
      SkipNode* prev_p = update[i]->forward[i].load();
      
      if(i == 0 && !CanInsert(node->item.first, node->item.second)) return REDUPLICATE; 
      if(prev_node->deleted.load()) return DELETED;

      node->forward[i].store(prev_p);
      bool cas_result = update[i]->forward[i].compare_exchange_strong(prev_p, node);

      printf("node %p\n", node);
      printf("update %p\n", update[i]->forward[i].load());

      return cas_result ? NO_ERROR : CAS_FAILED;
    }

    bool InstallNodes(const KeyType &key, const ValueType &value, int level) {
      SkipNode* node = new SkipNode(std::make_pair(key, value), level);
      SkipNode* update[MAX_LEVEL] = {nullptr};
      ERROR cas_result;

      while(1) { 
        UpdateList(key, update);
        cas_result = InstallNode(node, update, 0);
        if(cas_result == REDUPLICATE) {
          delete node;
          return false;
        }
        else if(cas_result == CAS_FAILED || cas_result == DELETED) {
          continue;
        }
        else {
          break;
        }
      }

      for(int i = 1; i <= level; ) {
        cas_result = InstallNode(node, update, i);
        if(cas_result == CAS_FAILED || cas_result == DELETED) {
          UpdateList(key, update); 
        }
        else {
          i++;
        }
      }
      
      return true;
    }

    bool Insert(const KeyType &key, const ValueType &value) {
      if(!CanInsert(key, value)) return false;

      int level = RandomLevel();
      if(level > cur_level) cur_level = level;

      return InstallNodes(key, value, level);
    }

    bool ConditionalInsert(const KeyType &key,
                            const ValueType &value,
                            std::function<bool(const void *)> predicate,
                            bool *predicate_satisfied) {
      SkipNode* node_p = ConditionalFind(key, value, predicate, predicate_satisfied);
      if(*predicate_satisfied == true) {
        return false;
      } else if(node_p != nullptr) {
        return false;
      }

      int level = RandomLevel();
      if(level > cur_level) cur_level = level;

      return InstallNodes(key, value, level);
    }

    bool DeleteNode(SkipNode* node, SkipNode* update[MAX_LEVEL], int i) {
      return update[i]->forward[i].compare_exchange_strong(node, node->forward[i].load());
    }

    bool Delete(const KeyType &key, const ValueType &value) {
      SkipNode* node = Find(key, value);
      SkipNode* update[MAX_LEVEL] = {nullptr};
      bool cas_result;
      bool deleted = node->deleted.load();
      if(node == nullptr || deleted) {
        return false;
      }
      else {
        node->deleted.compare_exchange_strong(deleted, true);
      }

      for(int i = node->level; i >= 0;) {
        UpdateList(key, value, update);
        cas_result = DeleteNode(node, update, node->level);
        if(!cas_result) {
          continue;
        }
        else {
          i--;
        }
      }

      ResetCurLevel();
      return true;
    }

    class ForwardIterator{
      private:
        SkipNode* cursor;
        SkipList* sl_p;
      public:
        ForwardIterator(SkipList *s_list_p) {
          assert(s_list_p != nullptr);
          sl_p = s_list_p;
          cursor = s_list_p->head->forward[0].load();
        }

        ForwardIterator(SkipList *s_list_p, const KeyType &start_key) {
          assert(s_list_p != nullptr);
          sl_p = s_list_p;
          cursor = sl_p->MoveTo(start_key);
        }

        ForwardIterator &operator=(const ForwardIterator &other) {
          if(this == &other) {
            return *this;
          }

          cursor = other.cursor;
          return *this;
        }

        bool IsEnd() const {
          if(cursor->forward[0].load() == nullptr) {
            return true;
          }
          else {
            return false;
          }
        }

        inline const KeyValuePair &operator*() {
          return cursor->item;
        }

        inline const KeyValuePair *operator->() {
          return &(cursor->item);
        }

        inline bool operator<(const ForwardIterator &other) const {
          if(other.IsEnd() == true) {
            if(IsEnd() == true) {
              return false; 
            } else {
              return true; 
            }
          } else if(IsEnd() == true) {
            return false; 
          }

          return sl_p->KeyCmpLess(cursor->item.first, other.cursor->item.first);
        }

        inline bool operator==(const ForwardIterator &other) const {
          if(other.IsEnd() == true) {
            if(IsEnd() == true) {
              // Two end iterators are equal to each other
              return true;
            } else {
              // Otherwise they are not equal
              return false;
            }
          } else if(IsEnd() == true) {
            return false;
          }
          
          return sl_p->KeyCmpEqual(cursor->item.first, other.cursor->item.first);
        }

        inline ForwardIterator &operator++() {
          if(IsEnd() == true) {
            return *this;
          }

          cursor = cursor->forward[0].load();

          return *this;
        }

        inline ForwardIterator operator++(int) {
          if(IsEnd() == true) {
            return *this;
          }

          ForwardIterator temp = *this;

          cursor = cursor->forward[0].load();

          return temp;
        }
    };
};
}  // End index namespace
}  // End peloton namespace
