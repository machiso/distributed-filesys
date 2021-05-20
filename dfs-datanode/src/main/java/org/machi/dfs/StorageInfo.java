package org.machi.dfs;

import java.util.ArrayList;
import java.util.List;

public class StorageInfo {

	private List<String> filenames = new ArrayList<String>();
	private Long storedDataSize = 0L;

	public List<String> getFilenames() {
		return filenames;
	}
	public void setFilenames(List<String> filenames) {
		this.filenames = filenames;
	}
	public Long getStoredDataSize() {
		return storedDataSize;
	}
	public void setStoredDataSize(Long storedDataSize) {
		this.storedDataSize = storedDataSize;
	}
	
	public void addFilename(String filename) {
		this.filenames.add(filename);
	}
	public void addStoredDataSize(Long storedDataSize) {
		this.storedDataSize += storedDataSize;
	}
	
}
