package com.org.entity;

import java.util.Collection;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;

@Entity
public class PrivilegeEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private String name;

	@ManyToMany(mappedBy = "privileges")
	private Collection<RoleEntity> rolesEntity;

	public PrivilegeEntity() {
		super();
		// TODO Auto-generated constructor stub
	}

	public PrivilegeEntity(Long id, String name, Collection<RoleEntity> rolesEntity) {
		super();
		this.id = id;
		this.name = name;
		this.rolesEntity = rolesEntity;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Collection<RoleEntity> getRoles() {
		return rolesEntity;
	}

	public void setRoles(Collection<RoleEntity> roles) {
		this.rolesEntity = roles;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((rolesEntity == null) ? 0 : rolesEntity.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PrivilegeEntity other = (PrivilegeEntity) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (rolesEntity == null) {
			if (other.rolesEntity != null)
				return false;
		} else if (!rolesEntity.equals(other.rolesEntity))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PrivilegeEntity [id=" + id + ", name=" + name + ", roles=" + rolesEntity + "]";
	}

}
