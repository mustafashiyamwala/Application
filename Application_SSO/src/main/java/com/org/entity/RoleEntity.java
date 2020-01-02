package com.org.entity;

import java.util.Collection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;

@Entity
public class RoleEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private String name;

	@ManyToMany(mappedBy = "roles")
	private Collection<RegistrationEntity> registrationEntities;

	@ManyToMany(fetch = FetchType.EAGER)
	@JoinTable(name = "Roles_Privileges", joinColumns = @JoinColumn(name = "roleId", referencedColumnName = "id"), inverseJoinColumns = @JoinColumn(name = "privilegeId", referencedColumnName = "id"))
	private Collection<PrivilegeEntity> privileges;

	public RoleEntity() {
		super();
		// TODO Auto-generated constructor stub
	}

	public RoleEntity(Long id, String name, Collection<RegistrationEntity> registrationEntities,
			Collection<PrivilegeEntity> privileges) {
		super();
		this.id = id;
		this.name = name;
		this.registrationEntities = registrationEntities;
		this.privileges = privileges;
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

	public Collection<RegistrationEntity> getRegistrationEntities() {
		return registrationEntities;
	}

	public void setRegistrationEntities(Collection<RegistrationEntity> registrationEntities) {
		this.registrationEntities = registrationEntities;
	}

	public Collection<PrivilegeEntity> getPrivileges() {
		return privileges;
	}

	public void setPrivileges(Collection<PrivilegeEntity> privileges) {
		this.privileges = privileges;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((registrationEntities == null) ? 0 : registrationEntities.hashCode());
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
		RoleEntity other = (RoleEntity) obj;
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
		if (registrationEntities == null) {
			if (other.registrationEntities != null)
				return false;
		} else if (!registrationEntities.equals(other.registrationEntities))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RoleEntity [id=" + id + ", name=" + name + "]";
	}

}