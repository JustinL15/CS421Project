package structures;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeNode<T extends Comparable<T>> implements HardwarePage {
    private boolean isLeaf;
    private List<T> keys;
    private List<int[]> pointers;
    private BPlusTreeNode<T> parent;
    private List<BPlusTreeNode<T>> children;
    private int maxSize;
    private Table template;

    public BPlusTreeNode(boolean isLeaf, int maxSize, Table template) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.pointers = new ArrayList<>();
        this.parent = null;
        this.children = new ArrayList<>();
        this.maxSize = maxSize;
        this.template = template;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public List<T> getKeys() {
        return keys;
    }

    public void setKeys(List<T> keys) {
        this.keys = keys;
    }

    public List<int[]> getPointers() {
        return pointers;
    }

    public void setPointers(List<int[]> pointers) {
        this.pointers = pointers;
    }

    public BPlusTreeNode<T> getParent() {
        return parent;
    }

    public void setParent(BPlusTreeNode<T> parent) {
        this.parent = parent;
    }

    public List<BPlusTreeNode<T>> getChildren() {
        return children;
    }

    public void setChildren(List<BPlusTreeNode<T>> children) {
        this.children = children;
    }

    public BPlusTreeNode<T> insert(T key, int[] pointer, Buffer buffer) {
        if (isLeaf) {
            int index = binarySearch(keys, key);
            keys.add(index, key);
            pointers.add(index, pointer);
            if (keys.size() > maxSize) {
                List<List<T>> splitKeys = splitArrayList(keys);
                List<List<int[]>> splitPointers = splitArrayList(pointers);
                this.keys = splitKeys.get(0);
                this.pointers = splitPointers.get(0);
                BPlusTreeNode<T> newNode = new BPlusTreeNode<T>(this.isLeaf, this.maxSize, this.template);
                newNode.setKeys(splitKeys.get(1));
                newNode.setPointers(splitPointers.get(1));
                newNode.setParent(this.parent);
                return newNode;
            }
            return null;
        } else {
            int index = binarySearch(keys, key);
            int[] pointerFromIndex = pointers.get(index);
            BPlusTreeNode<T> retrievedNode = buffer.read(template.getName(), pointerFromIndex[0], true);
            BPlusTreeNode<T> newNode = retrievedNode.insert(key, pointer, buffer);
            if (newNode != null) {
                keys.add(index, newNode.keys.get(0));
                if (index + 1 == keys.size()) {
                    pointers.add(newNode.pointers.get(0));
                } else {
                    pointers.add(index + 1, newNode.pointers.get(0));
                }
                if (keys.size() > maxSize - 1) {
                    List<List<T>> splitKeys = splitArrayList(keys);
                    List<List<int[]>> splitPointers = splitArrayList(pointers);
                    this.keys = splitKeys.get(0);
                    this.pointers = splitPointers.get(0);
                    BPlusTreeNode<T> newNode2 = new BPlusTreeNode<T>(this.isLeaf, this.maxSize, this.template);
                    newNode2.setKeys(splitKeys.get(1));
                    newNode2.setPointers(splitPointers.get(1));
                    newNode2.setParent(this.parent);
                    return newNode2;
                }
            }
            return null;
        }
    }

    public boolean delete(T key, Buffer buffer, List<T> keysDisplaced, List<int[]> pointersDisplaced) throws Exception {
        if (isLeaf) {
            int index = keys.indexOf(key);
            if (index != -1) {
                keys.remove(index);
                pointers.remove(index);
                if (keys.size() < maxSize / 2) {
                    if (keysDisplaced != null && pointersDisplaced != null) {
                        collectChildren(keysDisplaced, pointersDisplaced, buffer);
                    }
                    return true;
                }
            } else {
                throw new Exception("Key not found.");
            }
            return false;
        } else {
            int index = binarySearch(keys, key);
            BPlusTreeNode<T> nextNode = buffer.read(template.getName(), pointers.get(index)[0], true);
            ArrayList<T> keysD = new ArrayList<>();
            ArrayList<int[]> pointersD = new ArrayList<>();
            if (nextNode.delete(key, buffer, keysD, pointersD)) {
                pointers.remove(index);
                if (keys.size() == index) {
                    keys.remove(index - 1);
                } else {
                    keys.remove(index);
                }
                for (int i = 0; i < keysD.size(); i++) {
                    BPlusTreeNode<T> newNode = this.insert(keysD.get(i), pointersD.get(i), buffer);
                    if (newNode != null) {
                        // Write it to buffer
                        // Add to keys and pointers
                    }
                }
                if (keys.size() < maxSize / 2) {
                    if (keysDisplaced != null && pointersDisplaced != null) {
                        collectChildren(keysDisplaced, pointersDisplaced, buffer);
                    }
                    return true;
                }
            }
            return false;
        }
    }

    public int search(int key) {
        if (isLeaf) {
            int index = keys.indexOf(key);
            return (index != -1) ? pointers.get(index)[0] : -1;
        } else {
            int index = 0;
            while (index < keys.size() && keys.get(index) <= key) {
                index++;
            }
            return children.get(index).search(key);
        }
    }

    public void collectChildren(List<T> keysCollected, List<int[]> pointersCollected, Buffer buffer) {
        if (isLeaf) {
            for (int i = 0; i < keys.size(); i++) {
                keysCollected.add(keys.get(i));
                pointersCollected.add(pointers.get(i));
            }
        } else {
            for (int i = 0; i < pointers.size(); i++) {
                BPlusTreeNode<T> nextNode = buffer.read(template.getName(), pointers.get(i)[0], true);
                nextNode.collectChildren(keysCollected, pointersCollected, buffer);
            }
        }
    }

    public static <T extends Comparable<T>> int binarySearch(List<T> arrayList, T value) {
        if (arrayList.isEmpty()) {
            return -1;
        }
        int left = 0;
        int right = arrayList.size() - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            T midValue = arrayList.get(mid);
            int comparison = midValue.compareTo(value);
            if (comparison == 0) {
                return mid;
            } else if (comparison < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return -(left + 1);
    }
    
    

    public static <T> List<List<T>> splitArrayList(List<T> arrayList) {
        int mid = arrayList.size() / 2;
        List<List<T>> result = new ArrayList<>();
        result.add(arrayList.subList(0, mid));
        result.add(arrayList.subList(mid, arrayList.size()));
        return result;
    }

}
