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
#include <thread>
#include <cassert>
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
    class EpochManager;

    using KeyValuePair = std::pair<KeyType, ValueType>;

    using AtomSkipNode = std::atomic<SkipNode*>;

    using EpochNode = typename EpochManager::EpochNode;

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

        ~SkipNode() {
          delete [] forward;
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

    EpochManager epoch_manager;

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
      value_eq_obj{p_value_eq_obj},
      epoch_manager{this} {
      KeyType dummy_key;
      ValueType dummy_value = nullptr;

      head = new SkipNode(std::make_pair(dummy_key, dummy_value),MAX_LEVEL);
      tail = new SkipNode(std::make_pair(dummy_key, dummy_value),MAX_LEVEL);

      for(int i = 0; i <= MAX_LEVEL; i++) {
        head->forward[i].store(tail);
      }
    }

    ~SkipList() {
      delete head;
      delete tail;
    }

    inline bool IsTailOrNull(SkipNode* node) {
      return node == nullptr || node->forward[0].load() == nullptr;
    }   

    bool Insert(const KeyType &key, const ValueType &value) {
      EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

      bool result;
      if(!CanInsert(key, value)) {
        result =  false;
      }
      else {
        int level = RandomLevel();
        if(level > cur_level) cur_level = level;

        result = InsertNodes(key, value, level);
      }

      epoch_manager.LeaveEpoch(epoch_node_p);
      return result;
    }

    bool ConditionalInsert(const KeyType &key,
                            const ValueType &value,
                            std::function<bool(const void *)> predicate,
                            bool *predicate_satisfied) {
      ConditionalFind(key, predicate, predicate_satisfied);
      if(*predicate_satisfied == true) return false;

      return Insert(key, value);
    }

    bool Delete(const KeyType &key, const ValueType &value) {
      EpochNode *epoch_node_p = epoch_manager.JoinEpoch();
      bool result = DeleteNodes(key, value);
      epoch_manager.LeaveEpoch(epoch_node_p);
      return result;
    }

    bool DeleteNodes(const KeyType &key, const ValueType &value) {
      SkipNode* node = Find(key, value);
      SkipNode* update[MAX_LEVEL] = {nullptr};
      bool cas_result, deleted;
      if(node == nullptr) {
        return false;
      }

      deleted = node->deleted.load();
      if(deleted) return false;

      cas_result = node->deleted.compare_exchange_strong(deleted, true);
      if(!cas_result) return false;

      UpdateList(key, value, update, node->level);
      for(int i = node->level; i >= 0;) {
        cas_result = DeleteNode(node, update, i);
        if(!cas_result) {
          UpdateList(key, value, update, node->level);
          continue;
        }
        else {
          i--;
        }
      }

      epoch_manager.AddGarbageNode(node);
      ResetCurLevel();
      return true;
    }

    bool InsertNodes(const KeyType &key, const ValueType &value, int level) {
      SkipNode* node = new SkipNode(std::make_pair(key, value), level);
      SkipNode* update[MAX_LEVEL] = {nullptr};
      ERROR cas_result;

      while(1) { 
        UpdateList(key, update);
        cas_result = InsertNode(node, update, 0);
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
        cas_result = InsertNode(node, update, i);
        if(cas_result == CAS_FAILED || cas_result == DELETED) {
          UpdateList(key, update); 
        }
        else {
          i++;
        }
      }
      
      return true;
    }


    ERROR InsertNode(SkipNode* node, SkipNode* update[MAX_LEVEL], int i) {
      SkipNode* prev_node = update[i];
      SkipNode* prev_p = update[i]->forward[i].load();
      
      if(i == 0 && !CanInsert(node->item.first, node->item.second)) return REDUPLICATE; 
      if(prev_node->deleted.load()) return DELETED;

      node->forward[i].store(prev_p);
      bool cas_result = update[i]->forward[i].compare_exchange_strong(prev_p, node);

      return cas_result ? NO_ERROR : CAS_FAILED;
    }

    bool DeleteNode(SkipNode* node, SkipNode* update[MAX_LEVEL], int i) {
      return update[i]->forward[i].compare_exchange_strong(node, node->forward[i].load());
    }

    bool CanInsert(const KeyType &key, const ValueType &value) {
      SkipNode* node_p = unique_keys ? MoveTo(key) : Find(key, value);
      if(IsTailOrNull(node_p)) {
        return true;
      }
      else {
        return false;
      }
    }

    int RandomLevel () {
      int v = 1;

      while ((((double)std::rand() / RAND_MAX)) < 0.5 && std::abs(v) < MAX_LEVEL) {
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
      
    void UpdateList(const KeyType &key, const ValueType &value, SkipNode* update[MAX_LEVEL], int level) {
      SkipNode* prev = head;
      SkipNode* x = head;
      for(int i = level; i >= 0; i--) {
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
        while (!IsTailOrNull(x->forward[i].load()) && KeyCmpGreater(key, x->forward[i].load()->item.first)) {
          x = x->forward[i].load();
        }
      }

      if(x == head) x = x->forward[0].load();
      while(!IsTailOrNull(x)) { 
        if(KeyCmpLessEqual(key, x->item.first)) {
          break;
        }
        else {
          x = x->forward[0].load();
        }
      }

      return x;
    }

    void GetValue(const KeyType &key, std::vector<ValueType> &result) {
      EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

      SkipNode* x = MoveTo(key);
      while(!IsTailOrNull(x) && KeyCmpEqual(x->item.first, key) == true) {
        result.push_back(x->item.second);
        x = x->forward[0].load();
      }

      epoch_manager.LeaveEpoch(epoch_node_p);
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

    void ConditionalFind(const KeyType &key,
                              std::function<bool(const void *)> predicate,
                              bool *predicate_satisfied) {
      std::vector<ValueType> result;
      GetValue(key, result);

      *predicate_satisfied = false;
      for (auto it = begin (result); it != end (result); it++) {
        if(predicate((*it)) == true) {
          *predicate_satisfied = true;
          break;
        }
      }
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

        inline const SkipNode* operator*() {
          return cursor;
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

    class EpochManager {
      private:
        SkipList *slist_p;

      public:
        struct GarbageNode {
          const SkipNode *node_p;

          GarbageNode *next_p;
        };

        struct EpochNode {
          std::atomic<int> active_thread_count;

          std::atomic<GarbageNode *> garbage_list_p;

          EpochNode *next_p;
        };

        EpochNode *head_epoch_p;
        EpochNode *current_epoch_p;

        EpochManager(SkipList *p_slist_p) :
          slist_p{p_slist_p} {
          current_epoch_p = new EpochNode();

          current_epoch_p->active_thread_count.store(0);
          current_epoch_p->garbage_list_p.store(nullptr);
          current_epoch_p->next_p = nullptr;

          head_epoch_p = current_epoch_p;

          return;
        }

        ~EpochManager() {
          current_epoch_p = nullptr;

          ClearEpoch();
          
          if(head_epoch_p != nullptr) {
            for(EpochNode *epoch_node_p = head_epoch_p;
                epoch_node_p != nullptr;
                epoch_node_p = epoch_node_p->next_p) {
              epoch_node_p->active_thread_count = 0;
            }

            ClearEpoch();
          }
        }

        void CreateNewEpoch() {
          EpochNode *epoch_node_p = new EpochNode{};

          epoch_node_p->active_thread_count.store(0);
          epoch_node_p->garbage_list_p.store(nullptr);
          epoch_node_p->next_p = nullptr;

          current_epoch_p->next_p = epoch_node_p;
          current_epoch_p = epoch_node_p;

          return;
        }

        void AddGarbageNode(const SkipNode *node_p) {
          EpochNode *epoch_p = current_epoch_p;

          GarbageNode *garbage_node_p = new GarbageNode;
          garbage_node_p->node_p = node_p;
          garbage_node_p->next_p = epoch_p->garbage_list_p.load();

          while(1) {
            bool ret = epoch_p->garbage_list_p.compare_exchange_strong(garbage_node_p->next_p, garbage_node_p);

            if(ret == true) {
              break;
            } 
          }

          return;
        } 

        inline EpochNode *JoinEpoch() {
          EpochNode *epoch_p;
          do {
            epoch_p = current_epoch_p;

            int64_t prev_count = epoch_p->active_thread_count.fetch_add(1);

            if(prev_count < 0) {
              epoch_p->active_thread_count.fetch_sub(1);
            }
            else {
              break;
            }
          } while(1);

          return epoch_p;
        }

        inline void LeaveEpoch(EpochNode *epoch_p) {
          epoch_p->active_thread_count.fetch_sub(1);
          return;
        }
        
        void PerformGarbageCollection() {
          CreateNewEpoch();
          ClearEpoch();
          
          return;
        }

        void ClearEpoch() {
          while(1) {
            if(head_epoch_p == current_epoch_p) {
              break;
            }

            int active_thread_count = head_epoch_p->active_thread_count.load();
            assert(active_thread_count >= 0);

            if(active_thread_count != 0) {
              break;
            }

            const GarbageNode *next_garbage_node_p = nullptr;

            for(const GarbageNode *garbage_node_p = head_epoch_p->garbage_list_p.load();
                garbage_node_p != nullptr;
                garbage_node_p = next_garbage_node_p) {
              FreeSkipNode(garbage_node_p->node_p);
              next_garbage_node_p = garbage_node_p->next_p;
              delete garbage_node_p;
            } 

            EpochNode *next_epoch_node_p = head_epoch_p->next_p;
            delete head_epoch_p;
            head_epoch_p = next_epoch_node_p;
          } 

          return;
        }

        void FreeSkipNode(const SkipNode *node_p) {
          delete node_p;
        }

        size_t GetMemoryFootprint() {
          return GetActiveNodeMemory() + GetDeadNodeMemory();
        }

        size_t GetActiveNodeMemory() {
          size_t size = 0;
          for (auto scan_itr = slist_p->Begin(); scan_itr.IsEnd() == false; scan_itr++) {
            size += sizeof(SkipNode*) + sizeof(SkipNode) + ((*scan_itr)->level + 1) * sizeof(AtomSkipNode);
          }
          return size;
        }

        size_t GetDeadNodeMemory() {
          size_t size = 0;
          const GarbageNode *next_garbage_node_p = nullptr;

          for(EpochNode *epoch_node_p = head_epoch_p;
              epoch_node_p != nullptr;
              epoch_node_p = epoch_node_p->next_p) {
            for(const GarbageNode *garbage_node_p = epoch_node_p->garbage_list_p.load();
                garbage_node_p != nullptr;
                garbage_node_p = next_garbage_node_p) {
              size += sizeof(SkipNode*) + sizeof(SkipNode) + (garbage_node_p->node_p->level + 1) * sizeof(AtomSkipNode);
              next_garbage_node_p = garbage_node_p->next_p;
            } 
          }

          return size;
        }
    };

    size_t GetMemoryFootprint() {
      return epoch_manager.GetMemoryFootprint();
    }

    void PerformGarbageCollection() {
      epoch_manager.PerformGarbageCollection();
      return;
    }

    bool NeedGarbageCollection() {
      return (epoch_manager.GetDeadNodeMemory() > 0);
    }
};
}  // End index namespace
}  // End peloton namespace
