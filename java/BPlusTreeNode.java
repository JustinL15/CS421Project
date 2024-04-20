import java.util.ArrayList;
import java.util.List;

public class BPlusTreeNode {
    private boolean isLeaf;
    private List<Integer> keys;
    private List<Integer> values;
    private BPlusTreeNode parent;
    private List<BPlusTreeNode> children;

    public BPlusTreeNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
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

    public List<Integer> getValues() {
        return values;
    }

    public void setValues(List<Integer> values) {
        this.values = values;
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

    
    public void insert(int key, int value) {
        if (isLeaf) {
            int index = 0;
            while (index < keys.size() && keys.get(index) < key) {
                index++;
            }
            keys.add(index, key);
            values.add(index, value);
        } else {
            int index = 0;
            while (index < keys.size() && keys.get(index) < key) {
                index++;
            }
            children.get(index).insert(key, value);
        }

    }

    public void delete(int key) {
        
    }

    public void search(int key) {
        
    }
}