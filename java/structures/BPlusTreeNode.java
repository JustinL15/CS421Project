import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BPlusTreeNode implements HardwarePage {
    private boolean isLeaf;
    private List<Object> keys;
    private List<int[]> pointers;
    private int parent;
    private int maxSize;
    private Table template;
    private int pageNumber;

    public BPlusTreeNode(boolean isLeaf, int maxSize, Table template) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.pointers = new ArrayList<>();
        if (isLeaf) {
            pointers.add(new int[] {-1, -1}); //next pointer initalized to "null"
        }
        this.parent = -1;
        this.maxSize = maxSize;
        this.template = template;
        this.pageNumber = template.getNextFreePage();
    }

    public BPlusTreeNode(byte[] data, int maxSize, Table template, int pageNumber) {
        this.pageNumber = pageNumber;
        this.template = template;
        this.maxSize = maxSize;
        this.parent = -1;
        this.keys = new ArrayList<>();

        ByteBuffer buffer = ByteBuffer.wrap(data);

        int keySize = buffer.getInt();
        Type keyType = template.getAttributes().get(template.getPrimaryKeyIndex()).getDataType();

        for (int i = 0; i < keySize; i++) {
            switch (keyType) {
                case Integer:
                    this.keys.add(buffer.getInt());
                    break;
                case Double:
                    this.keys.add(buffer.getDouble());
                    break;
                case Boolean:
                    this.keys.add(buffer.get() == 1);
                    break;
                case Char:
                    int clength = template.getAttributes().get(template.getPrimaryKeyIndex()).getMaxLength();
                    char[] charArr = new char[clength];
    
                    for (int j = 0; j < clength; j++) {
                        charArr[j] = buffer.getChar();
                    }
                    this.keys.add(String.valueOf(charArr));
                    break;
                case Varchar:
                    int vlength = template.getAttributes().get(template.getPrimaryKeyIndex()).getMaxLength();
                    char[] vcharArr = new char[vlength];
    
                    for (int j = 0; j < vlength; j++) {
                        vcharArr[j] = buffer.getChar();
                    }
                    this.keys.add(String.valueOf(vcharArr));
                    break;
                }
        }

        int pointerSize = buffer.getInt();

        for (int i = 0; i < pointerSize; i++) {
            int[] pointer = new int[2];
            pointer[0] = buffer.getInt();
            pointer[1] = buffer.getInt();
            pointers.add(pointer);
        }
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public List<Object> getKeys() {
        return keys;
    }

    public void setKeys(List<Object> keys) {
        this.keys = keys;
    }

    public List<int[]> getPointers() {
        return pointers;
    }

    public void setPointers(List<int[]> pointers) {
        this.pointers = pointers;
    }

    public int getParent() {
        return parent;
    }

    public void setParent(int parent) {
        this.parent = parent;
    }

    public int[] insert(Object key, Buffer buffer) throws Exception {
        if (isLeaf) {
            int index = binarySearch(keys, key);
            keys.add(index, key);
            int[] newPointer = new int[]{0, 0};
            if (pointers.size() == 0) {
                pointers.add(newPointer);
            } else {
                if (index == pointers.size() - 1) {
                    newPointer[0] = pointers.get(index - 1)[0];
                    newPointer[1] = pointers.get(index - 1)[1] + 1;
                } else {
                    newPointer[0] = pointers.get(index)[0];
                    newPointer[1] = pointers.get(index)[1];
                }
                pointers.add(newPointer);
                updatePointers(index, newPointer[0], buffer);
            }

            if (pointers.size() > maxSize) {
                splitLeafNode(buffer, index);
            }
            return newPointer;
        } else {
            int index = binarySearch(keys, key);
            BPlusTreeNode retrievedNode = (BPlusTreeNode) buffer.read(template.getName(), pointers.get(index)[0], true);
            retrievedNode.parent = pageNumber;
            int[] newPointer = retrievedNode.insert(key, buffer);
            if (pointers.size() > maxSize) {
                splitInternalNode(buffer, index);
            }
            return newPointer;
        }
    }

    private void splitLeafNode(Buffer buffer, int index) {
        int splitIndex = keys.size() / 2;
        List<Object> newKeys = keys.subList(splitIndex, keys.size());
        List<int[]> newPointers = pointers.subList(splitIndex, pointers.size() - 1);
        keys.subList(splitIndex, keys.size()).clear();
        pointers.subList(splitIndex, pointers.size() - 1).clear();
        BPlusTreeNode newNode = new BPlusTreeNode(true, maxSize, template);
        newNode.setKeys(newKeys);
        newNode.setPointers(newPointers);
        newNode.getPointers().get(newNode.getPointers().size() - 1)[0] = this.pointers.get(pointers.size() - 1)[0];
        this.pointers.get(pointers.size() - 1)[0] = newNode.getPageNumber();
        if (this.parent == -1) {
            BPlusTreeNode newRoot = new BPlusTreeNode(false, maxSize, template);
            newRoot.keys.add(newNode.keys.get(0));
            newRoot.pointers.add(new int[] {this.pageNumber, -1});
            newRoot.pointers.add(new int[] {newNode.pageNumber, -1});
            this.parent = newRoot.pageNumber;
            newNode.parent = newRoot.pageNumber;
            template.setRootPage(newRoot.pageNumber);
            buffer.addPage(newRoot);
        } else {
            BPlusTreeNode nodeParent = (BPlusTreeNode) buffer.read(template.getName(), this.parent, true);
            nodeParent.keys.add(index + 1, newNode.keys.get(0));
            nodeParent.pointers.add(index + 1, new int[] {newNode.pageNumber, -1});
        }
        buffer.addPage(newNode);
    }

    private void splitInternalNode(Buffer buffer, int index) {
        int splitIndex = keys.size() / 2;
        Object midVal = keys.get(splitIndex);
        List<Object> newKeys = keys.subList(splitIndex + 1, keys.size());
        List<int[]> newPointers = pointers.subList(splitIndex, pointers.size());
        keys.subList(splitIndex, keys.size()).clear();
        pointers.subList(splitIndex, pointers.size()).clear();
        BPlusTreeNode newNode = new BPlusTreeNode(false, maxSize, template);
        newNode.setKeys(newKeys);
        newNode.setPointers(newPointers);
        if (this.parent == -1) {
            BPlusTreeNode newRoot = new BPlusTreeNode(false, maxSize, template);
            newRoot.keys.add(midVal);
            newRoot.pointers.add(new int[] {this.pageNumber, -1});
            newRoot.pointers.add(new int[] {newNode.pageNumber, -1});
            this.parent = newRoot.pageNumber;
            newNode.parent = newRoot.pageNumber;
            template.setRootPage(newRoot.pageNumber);
            buffer.addPage(newRoot);
        } else {
            BPlusTreeNode nodeParent = (BPlusTreeNode) buffer.read(template.getName(), this.parent, true);
            nodeParent.keys.add(index + 1, newNode.keys.get(0));
            nodeParent.pointers.add(index + 1, new int[] {newNode.pageNumber, -1});
        }
        buffer.addPage(newNode);
    }

    public boolean delete(Object key, Buffer buffer) throws Exception {
        if (isLeaf) {
            int index = keys.indexOf(key);
            if (index != -1) {
                keys.remove(index);
                pointers.remove(index);
                if (keys.size() < maxSize / 2) {
                    return true;
                }
            } else {
                throw new Exception("Key not found.");
            }
            return false;
        } else {
            int index = binarySearch(keys, key);
            BPlusTreeNode nextNode = (BPlusTreeNode) buffer.read(template.getName(), pointers.get(index)[0], true);
            nextNode.parent = this.pageNumber;
            if (nextNode.delete(key, buffer)) {
                keys.remove(index);
                pointers.remove(index);
                merge(index, buffer);
                if (this.parent == -1 && pointers.size() < 2) {
                    if (pointers.size() == 0) {
                        template.setRootPage(-1);
                    } else {
                        template.setRootPage(pointers.get(0)[0]);
                    }
                    return true;
                }
                if (keys.size() < maxSize / 2) {
                    return true;
                }
            }
            return false;
        }
    }

    // public int search(int key) {
    //     if (isLeaf) {
    //         int index = keys.indexOf(key);
    //         return (index != -1) ? pointers.get(index)[0] : -1;
    //     } else {
    //         int index = 0;
    //         while (index < keys.size() && ((Integer) keys.get(index)) <= key) {
    //             index++;
    //         }
    //         return children.get(index).search(key);
    //     }
    // }

    // returns pointer [-1, -1] if key does not exist in the tree
    public int[] search(Object key, Buffer buffer) throws Exception {
        if (isLeaf) {
            int index = binarySearch(keys, key);
            if (index != -1) {
                return pointers.get(index);
            } else {
                int[] dummyPointer = new int[2];
                dummyPointer[0] = -1;
                dummyPointer[1] = -1;
                return dummyPointer;
            }
        } else {
            int index = 0;
            while (index < keys.size() && compareVals(keys.get(index), (key)) <= 0) {
                index++;
            }
            BPlusTreeNode child = (BPlusTreeNode)buffer.read(template.getName(), pointers.get(index)[0], true);
            return child.search(key, buffer);
        }
    }

    public int binarySearch(List<Object> arrayList, Object key) throws Exception {
        if (arrayList.isEmpty()) {
            return -1;
        }
        int left = 0;
        int right = arrayList.size() - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            Object midValue = arrayList.get(mid);
            int comparison = compareVals(midValue, key);
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
        
        for (Object key : keys) {
            size += key.toString().getBytes().length;
        }
        
        for (int[] pointer : pointers) {
            size += pointer.length * Integer.BYTES;
        }

        return size;
    }


    public byte[] toBytes() {
        List<Byte> byteList = new ArrayList<>();

        for (Object key : keys) {
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

    @Override
    public byte[] toByte(int max_size) {
        int bytesPerKey = template.getMaxPKeySize(); 
        int totalBytes = keys.size() * bytesPerKey + pointers.size() * 8 + 8; // 8 for key size and pointer size integers

        ByteBuffer buffer = ByteBuffer.allocate(totalBytes);

        buffer.putInt(keys.size());
        for (Object key : keys) {
            String keyString = key.toString();
            byte[] keyBytes = keyString.getBytes();
            buffer.put(keyBytes);
            
            for (int i = keyBytes.length; i < bytesPerKey; i++) {
                buffer.put((byte) 0);
            }
        }
        
        buffer.putInt(pointers.size());
        for (int[] pointer : pointers) {
            for (int value : pointer) {
                buffer.putInt(value);
            }
        }

        return buffer.array();
    }

    @Override
    public Table getTemplate() {
        return this.template;        
    }

    public void updatePointers(int startIndex, int PageNumber, Buffer buffer) {
        for (int i = startIndex; i < pointers.size(); i++) {
            if (pointers.get(i)[1] == -1) {
                BPlusTreeNode nextNode = (BPlusTreeNode) buffer.read(template.getName(), pointers.get(i)[0], true);
                nextNode.updatePointers(0, PageNumber, buffer);
            } else if (pointers.get(i)[0] == pageNumber) {
                pointers.get(i)[1] += 1;
            } else {
                return;
            }
        }
    }

    public void merge(int index, Buffer buffer) {
        if (index != 0) {
            BPlusTreeNode underfull = (BPlusTreeNode) buffer.read(template.getName(), pointers.get(index)[0], true);
            BPlusTreeNode mergeNode = (BPlusTreeNode) buffer.read(template.getName(), pointers.get(index - 1)[0], true);
            mergeNode.keys.addAll(underfull.getKeys());
            mergeNode.pointers.addAll(underfull.getPointers());
            template.addFreePage(underfull.pageNumber);
            if (mergeNode.pointers.size() > maxSize) {
                if (isLeaf) {
                    mergeNode.splitLeafNode(buffer, index - 1);
                } else {
                    mergeNode.splitInternalNode(buffer, index - 1);
                }
            }
        } else {
            BPlusTreeNode underfull = (BPlusTreeNode) buffer.read(template.getName(), pointers.get(index)[0], true);
            BPlusTreeNode mergeNode = (BPlusTreeNode) buffer.read(template.getName(), pointers.get(index + 1)[0], true);
            mergeNode.keys.addAll(underfull.getKeys());
            mergeNode.pointers.addAll(underfull.getPointers());
            template.addFreePage(underfull.pageNumber);
            if (mergeNode.pointers.size() > maxSize) {
                if (isLeaf) {
                    mergeNode.splitLeafNode(buffer, index);
                } else {
                    mergeNode.splitInternalNode(buffer, index);
                }
            }
        }
    }

    public int compareVals(Object x, Object y) throws Exception {
        switch(template.getAttributes().get(template.getPrimaryKeyIndex()).getDataType()) {
            case Integer:
                return ((Integer) x).compareTo((Integer) y); 
            case Double:
                return ((Double) x).compareTo((Double) y);
            case Boolean:
                return ((Boolean) x).compareTo((Boolean) y);
            case Char:
            case Varchar:
                return ((String) x).compareTo((String) y);
            default:
                throw new Exception("Invalid Type for Primary key");
        }
    }

    public boolean updateNodePointer(Object key, Buffer buffer) throws Exception {
        if (isLeaf) {
            int index = keys.indexOf(key);
            if (index != -1) {
                keys.set(index, key);
                pointers.set(index, );
                if (keys.size() < maxSize / 2) {
                    return true;
                }
            } else {
                throw new Exception("Key not found.");
            }
            return false;
        } else {
            int newIndex = binarySearch(keys, key);
            BPlusTreeNode nextNode = (BPlusTreeNode) buffer.read(template.getName(), pointers.get(newIndex)[0], true);
            nextNode.parent = this.pageNumber;
            if (nextNode.delete(key, buffer)) {
                keys.set(newIndex, key);
                pointers.set(newIndex, );
                merge(newIndex, buffer);
                if (keys.size() < maxSize / 2) {
                    return true;
                }
            }
            return false;
        }
    }

    public void insertIntoNode(BPlusTreeNode node, Buffer buffer) throws Exception {
        Object primaryKey = getPrimaryKey();
        if (primaryKey != null) {
            node.insert(primaryKey, buffer);
        } else {
            throw new Exception();
        }
    }

    public boolean deleteFromNode(BPlusTreeNode node, Buffer buffer) throws Exception {
        Object primaryKey = getPrimaryKey();
        if (primaryKey != null) {
            return node.delete(primaryKey, buffer);
        } else {
            throw new Exception();
        }
    }
}
