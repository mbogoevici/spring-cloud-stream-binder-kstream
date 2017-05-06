/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo.kstream;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Marius Bogoevici
 */
@ConfigurationProperties(prefix = "kstream.word.count")
public class WordCountProcessorProperties {

	private int windowLength = 5000;

	private int advanceBy = 0;

	private String storeName = "WordCounts";

	public int getWindowLength() {
		return windowLength;
	}

	public void setWindowLength(int windowLength) {
		this.windowLength = windowLength;
	}

	public int getAdvanceBy() {
		return advanceBy;
	}

	public void setAdvanceBy(int advanceBy) {
		this.advanceBy = advanceBy;
	}

	public String getStoreName() {
		return storeName;
	}

	public void setStoreName(String storeName) {
		this.storeName = storeName;
	}
}
