package structures;import java.nio.ByteBuffer;
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
    private int pageNumber;

    public BPlusTreeNode(boolean isLeaf, int maxSize, Table template) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.pointers = new ArrayList<>();
        this.parent = null;
        this.children = new ArrayList<>();
        this.maxSize = maxSize;
        this.template = template;
        this.pageNumber = -1; // Default value
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
                return splitLeafNode();
            }
        } else {
            int index = binarySearch(keys, key);
            BPlusTreeNode<T> retrievedNode = (BPlusTreeNode<T>) buffer.read(template.getName(), pointers.get(index)[0], true);
            BPlusTreeNode<T> newNode = retrievedNode.insert(key, pointer, buffer);
            if (newNode != null) {
                keys.add(index, newNode.keys.get(0));
                // Generate pointer
                if (keys.size() > maxSize - 1) {
                    return splitInternalNode();
                }
            }
        }
        return null; // Return null if no split occurred
    }

    private BPlusTreeNode<T> splitLeafNode() {
        int splitIndex = keys.size() / 2;
        List<T> newKeys = keys.subList(splitIndex, keys.size());
        List<int[]> newPointers = pointers.subList(splitIndex, pointers.size());
        keys.subList(splitIndex, keys.size()).clear();
        pointers.subList(splitIndex, pointers.size()).clear();
        BPlusTreeNode<T> newNode = new BPlusTreeNode<>(true, maxSize, template);
        newNode.setKeys(newKeys);
        newNode.setPointers(newPointers);
        newNode.setParent(this.parent);
        return newNode;
    }

    private BPlusTreeNode<T> splitInternalNode() {
        int splitIndex = (keys.size() + 1) / 2;
        List<T> newKeys = keys.subList(splitIndex, keys.size());
        List<int[]> newPointers = pointers.subList(splitIndex, pointers.size());
        keys.subList(splitIndex, keys.size()).clear();
        pointers.subList(splitIndex, pointers.size()).clear();
        BPlusTreeNode<T> newNode = new BPlusTreeNode<>(false, maxSize, template);
        newNode.setKeys(newKeys);
        newNode.setPointers(newPointers);
        newNode.setParent(this.parent);
        return newNode;
    }

    public int search(int key) {
        if (isLeaf) {
            int index = keys.indexOf(key);
            return (index != -1) ? pointers.get(index)[0] : -1;
        } else {
            int index = 0;
            while (index < keys.size() && ((Integer) keys.get(index)) <= key) {
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
                BPlusTreeNode<T> nextNode = (BPlusTreeNode<T>) buffer.read(template.getName(), pointers.get(i)[0], true);
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

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public void setPageNumber(int number) {
        this.pageNumber = number;
    }

    @Override
    public int bytesUsed() {
        int size = 0;
        
        for (T key : keys) {
            size += key.toString().getBytes().length;
        }
        
        for (int[] pointer : pointers) {
            size += pointer.length * Integer.BYTES;
        }
        
        for (BPlusTreeNode<T> child : children) {
            size += child.bytesUsed();
        }
        return size;
    }


    public byte[] toBytes() {
        List<Byte> byteList = new ArrayList<>();

        for (T key : keys) {
            byte[] keyBytes = key.toString().getBytes();
            for (byte b : keyBytes) {
                byteList.add(b);
            }
        }

        for (int[] pointer : pointers) {
            for (int value : pointer) {
                byteList.add((byte) (value >> 24));
                byteList.add((byte) (value >> 16));
                byteList.add((byte) (value >> 8));
                byteList.add((byte) value);
            }
        }

        byte[] byteArray = new byte[byteList.size()];
        for (int i = 0; i < byteList.size(); i++) {
            byteArray[i] = byteList.get(i);
        }

        return byteArray;
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
            BPlusTreeNode<T> nextNode = (BPlusTreeNode<T>) buffer.read(template.getName(), pointers.get(index)[0], true);
            List<T> keysD = new ArrayList<>();
            List<int[]> pointersD = new ArrayList<>();
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
                        buffer.write(newNode); // Write the new node to buffer
                        keys.add(keys.indexOf(keysD.get(i)), newNode.getKeys().get(0)); // Add new key to keys
                        pointers.add(keys.indexOf(keysD.get(i)), new int[]{newNode.getPageNumber()}); // Add new pointer to pointers
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


    @Override
    public byte[] toByte(int max_size) {
        int bytesPerKey = max_size / keys.size(); 
        int totalBytes = keys.size() * bytesPerKey + pointers.size() * 4;

        ByteBuffer buffer = ByteBuffer.allocate(totalBytes);
        for (T key : keys) {
            String keyString = key.toString();
            byte[] keyBytes = keyString.getBytes();
            buffer.put(keyBytes);
        
            for (int i = keyBytes.length; i < bytesPerKey; i++) {
                buffer.put((byte) 0);
            }
        }

        for (int[] pointer : pointers) {
            for (int value : pointer) {
                buffer.putInt(value);
            }
        }

        return buffer.array();
    }
    }

    @Override
    public Table getTemplate() {
        return this.template;        
    }
    
}
