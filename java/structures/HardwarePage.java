public interface HardwarePage {
    // BPlusTreeNode should have a constructor that takes a byte[] and decodes it
    // I believe it should also have a constructor that takes existing keys and pointers for when we split nodes
    public byte[] toByte(int max_size);
    public int bytesUsed();
    public Table getTemplate();
    public int getPageNumber();
    public void setPageNumber(int number);
}
