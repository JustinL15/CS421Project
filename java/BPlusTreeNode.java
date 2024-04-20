import java.util.ArrayList;
import java.util.List;

public class BPlusTreeNode implements HardwarePage {
    private boolean isLeaf;
    private List<Integer> keys;
    private List<int[]> pointers;
    private BPlusTreeNode parent;
    private List<BPlusTreeNode> children;

    public BPlusTreeNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.pointers = new ArrayList<>();
        this.parent = null;
        this.children = new ArrayList<>();
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

    
    public void insert(int key, int[] pointer) {
        if (isLeaf) {
            int index = 0;
            while (index < keys.size() && keys.get(index) < key) {
                index++;
            }
            keys.add(index, key);
            pointers.add(index, value);
        } else {
            int index = 0;
            while (index < keys.size() && keys.get(index) < key) {
                index++;
            }
            children.get(index).insert(key, pointers);
        }

    }

    public void delete(int key) {
        
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
}