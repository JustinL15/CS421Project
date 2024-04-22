package structures;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeNode implements HardwarePage {
    private boolean isLeaf;
    private List<Integer> keys;
    private List<int[]> pointers;
    private BPlusTreeNode parent;
    private List<BPlusTreeNode> children;
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

    public List<Integer> getKeys() {
        return keys;
    }

    public void setKeys(List<Integer> keys) {
        this.keys = keys;
    }

    public List<int[]> getPointers() {
        return pointers;
    }

    public void setPointers(List<int[]> pointers) {
        this.pointers = pointers;
    }

    public BPlusTreeNode getParent() {
        return parent;
    }

    public void setParent(BPlusTreeNode parent) {
        this.parent = parent;
    }

    public List<BPlusTreeNode> getChildren() {
        return children;
    }

    public void setChildren(List<BPlusTreeNode> children) {
        this.children = children;
    }

    
    public BPlusTreeNode insert(Object key, int[] pointer, Buffer buffer ) {
        if (isLeaf) {
            int index = binarySearch(this.keys, key);
            this.keys.add(index, key);
            this.pointers.add(index, key);
            if (keys.size() > maxSize) {
                List<List<object>> splitKeys = splitArrayList(keys);
                List<List<int[]>> splitPointers = splitArrayList(pointers)
                this.keys = splitKeys.get(0);
                this.pointers = splitPointers.get(0);
                BPlusTreeNode newNode = new BPlusTreeNode(this.isLeaf, this.maxSize);
                newNode.setKeys(splitKeys.get(1));
                newNode.setPointers(splitPointers.get(1));
                newNode.setParent(this.parent);
                return newNode;
            }
            return null;
        } else {
            int index = binarySearch(this.keys, key);
            int[] pointer = pointers.get(index);
            BPlusTreeNode retrievedNode = buffer.read(template.getName(), pointer[0], true);
            BPlusTreeNode newNode = retrievedNode.insert(key, pointer);
            if (newNode != null) {
                this.keys.add(index, newNode.keys.get(0));
                if (index + 1 = this.keys.size()) {
                    //new node pointer
                }
                //new node pointer
                if (keys.size() > maxSize - 1) {
                    List<List<object>> splitKeys = splitArrayList(keys);
                    List<List<int[]>> splitPointers = splitArrayList(pointers)
                    this.keys = splitKeys.get(0);
                    this.pointers = splitPointers.get(0);
                    BPlusTreeNode newNode = new BPlusTreeNode(this.isLeaf, this.maxSize);
                    newNode.setKeys(splitKeys.get(1));
                    newNode.setPointers(splitPointers.get(1));
                    newNode.setParent(this.parent);
                    return newNode;
                }
            }
            return null;
        }

    }

    public boolean delete(Object key, Buffer buffer, List<Object> keysDisplaced, List<Object> pointersDisplaced) throws Exception {
        if (isLeaf) {
            int index = keys.indexOf(key);
            if (index != -1) {
                keys.remove(index);
                pointers.remove(index);
                if (keys.size() < maxSize/2) {
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
            int index = binarySearch(this.keys, key);
            BPlusTreeNode nextNode = buffer.read(template.getName(), pointers.get(index)[0], true);
            ArrayList<Object> keysD = new ArrayList();
            ArrayList<int[]> pointersD = new ArrayList();
            if (nextNode.delete(key, buffer, keysD, pointersD)) {
                // add free'd space to catalog
                pointers.remove(index)
                if (keys.size() == index) {
                    keys.remove(index - 1)
                } else {
                    keys.remove(index)
                }
                for (int i = 0; i < keysD.size(), i++) {
                    BPlusTreeNode newNode = this.insert();
                    if (newNode != null) {
                        // write it to buffer
                        // add to keys and pointers
                }
                if (keys.size() < maxSize/2) {
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
            return (index != -1) ? pointers.get(index) : -1;
        } else {
            int index = 0;
            while (index < keys.size() && keys.get(index) <= key) {
                index++;
            }
            return children.get(index).search(key);
        }

    }

    public byte[] toByte(int max_size){return null;}
    public int bytesUsed(){return 0;}
    public Table getTemplate(){return null;}
    public int getPageNumber(){return 0;}
    public void setPageNumber(int number){}

    public static <T extends Comparable<? super T>> int binarySearch(ArrayList<T> arrayList, T value) {
        if (arrayList.isEmpty()) {
            return 0;
        }

        int left = 0;
        int right = arrayList.size() - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            T midValue = arrayList.get(mid);

            int comparison = value.compareTo(midValue);

            if (comparison == 0) {
                return mid + 1;
            } else if (comparison < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return left;
    }

    public static <T> List<List<T>> splitArrayList(ArrayList<T> arrayList) {
        int mid = arrayList.size() / 2;
        
        List<List<T>> result = new ArrayList<>();
        result.add(arrayList.subList(0, mid));
        result.add(arrayList.subList(mid, arrayList.size()));
        return result;
    }

    public void collectChildren(List<Object> keysCollected, List<Object> pointersCollected, Buffer buffer) {
        if (isLeaf) {
            for (int i = 0; i < keys.size(), i++) {
                keysDisplaced.add(keys.get(i));
                pointersDisplaced.add(pointers.get(i));
            }
        } else {
            for (int i = 0; i < pointers.size(), i++) {
                BPlusTreeNode nextNode = buffer.read(template.getName(), pointers.get(i)[0], true);
                nextNode.collectChildren(keysCollected, pointersCollected, buffer);
            }
        }
    }
}