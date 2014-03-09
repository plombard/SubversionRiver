/*
 * Copyright [2014] [Pascal Lombard]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.elasticsearch.river.subversion.crawler;

import com.google.common.base.Optional;

/**
 * Yet another POJO, to handle filtering of SVN entries
 */
@SuppressWarnings("unused")
public class LogEntryFilter {
    private boolean contentToBeFiltered = false;
    private boolean crawlingToBePrevented = false;
    private Optional<String> reason;

    public boolean contentToBeFiltered() {
        return contentToBeFiltered;
    }

    public boolean crawlingToBePrevented() {
        return crawlingToBePrevented;
    }

    public Optional<String> getReason() {
        return reason;
    }

    public LogEntryFilter(boolean contentToBeFiltered,
                          boolean crawlingToBePrevented,
                          String reason) {
        this.contentToBeFiltered = contentToBeFiltered;
        this.crawlingToBePrevented = crawlingToBePrevented;
        this.reason = Optional.fromNullable(reason);
    }
}
