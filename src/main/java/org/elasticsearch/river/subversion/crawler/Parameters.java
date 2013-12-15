/*
 * Copyright [2013] [Pascal Lombard]
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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Set;
import java.util.regex.Pattern;


/**
 * POJO for the crawler parameters
 */
@SuppressWarnings("unused")
public class Parameters {
    private final Optional<String> login;
    private transient final Optional<String> password;
    private final Optional<String> path;
    private Optional<Long> startRevision;
    private Optional<Long> endRevision;
    private final Optional<Long> maximumFileSize;
    private final ImmutableSet<Pattern> patternsToFilter;

    public Parameters(final Optional<String> login,
                      final Optional<String> password,
                      final Optional<String> path,
                      final Optional<Long> startRevision,
                      final Optional<Long> endRevision,
                      final Optional<Long> maximumFileSize,
                      final ImmutableSet<Pattern> patternsToFilter) {
        this.login = login;
        this.password = password;
        this.path = path;
        this.startRevision = startRevision;
        this.endRevision = endRevision;
        this.maximumFileSize = maximumFileSize;
        this.patternsToFilter = patternsToFilter;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("login", login)
            .add("path", path)
            .add("startRevision", startRevision)
            .add("endRevision", endRevision)
            .add("maximumFileSize", maximumFileSize)
            .add("patternsToFilter", Iterables.toString(patternsToFilter))
            .toString();
    }

    public Optional<String> getLogin() {
        return login;
    }

    public Optional<String> getPassword() {
        return password;
    }

    public Optional<String> getPath() {
        return path;
    }

    public Optional<Long> getStartRevision() {
        return startRevision;
    }

    public Optional<Long> getEndRevision() {
        return endRevision;
    }

    public Optional<Long> getMaximumFileSize() {
        return maximumFileSize;
    }

    public ImmutableSet<Pattern> getPatternsToFilter() {
        return patternsToFilter;
    }

    public void setStartRevision(Optional<Long> startRevision) {
        this.startRevision = startRevision;
    }

    public void setEndRevision(Optional<Long> endRevision) {
        this.endRevision = endRevision;
    }

    /**
     * Builder class for Parameters.
     * Default values are defined here.
     */
    public static class ParametersBuilder {
        private Optional<String> nestedLogin = Optional.of("anonymous");
        private transient Optional<String> nestedPassword = Optional.of("password");
        private Optional<String> nestedPath = Optional.of("/");
        private Optional<Long> nestedStartRevision = Optional.of(1L);
        private Optional<Long> nestedEndRevision = Optional.absent();
        private Optional<Long> nestedMaximumFileSize = Optional.absent();
        private ImmutableSet<Pattern> nestedPatternsToFilter = ImmutableSet.of();

        public ParametersBuilder setLogin(final String newLogin) {
            this.nestedLogin = Optional.fromNullable(newLogin).or(nestedLogin);
            return this;
        }

        public ParametersBuilder setPassword(final String newPassword) {
            this.nestedPassword = Optional.fromNullable(newPassword).or(nestedPassword);
            return this;
        }

        public ParametersBuilder setPath(final String newPath) {
            this.nestedPath = Optional.fromNullable(newPath).or(nestedPath);
            return this;
        }
        
        public ParametersBuilder setStartRevision(final Long newStartRevision) {
            this.nestedStartRevision = Optional.fromNullable(newStartRevision).or(nestedStartRevision);
            return this;
        }

        public ParametersBuilder setEndRevision(final Long newEndRevision) {
            this.nestedEndRevision = Optional.fromNullable(newEndRevision).or(nestedEndRevision);
            return this;
        }

        public ParametersBuilder setMaximumFileSize(final Long newMaximumFileSize) {
            this.nestedMaximumFileSize = Optional.fromNullable(newMaximumFileSize).or(nestedMaximumFileSize);
            return this;
        }

        public ParametersBuilder setPatternsToFilter(final Set<Pattern> newPatternsToFilter) {
            this.nestedPatternsToFilter = ImmutableSet.copyOf(newPatternsToFilter);
            return this;
        }

        public Parameters create() {
            return new Parameters(nestedLogin,
                    nestedPassword,
                    nestedPath,
                    nestedStartRevision,
                    nestedEndRevision,
                    nestedMaximumFileSize,
                    nestedPatternsToFilter);
        }
    }
}
